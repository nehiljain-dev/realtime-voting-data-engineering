# --- Windows / portable environment guard -------------------------------------
# Stale SPARK_HOME or HADOOP_HOME values are a very common cause of confusing
# Spark startup errors on Windows ("cannot find the file specified", winutils
# missing). Resolve them to the installed packages instead of failing.
import os as _os
import sys as _sys
from pathlib import Path as _Path


def _ensure_spark_env() -> None:
    spark_home = _os.environ.get("SPARK_HOME")
    if not spark_home or not _Path(spark_home).is_dir():
        import pyspark as _pyspark

        _os.environ["SPARK_HOME"] = str(_Path(_pyspark.__file__).resolve().parent)

    hadoop_home = _os.environ.get("HADOOP_HOME")
    if not hadoop_home or not (_Path(hadoop_home) / "bin" / "winutils.exe").exists():
        for candidate in (_Path.home() / "hadoop", _Path("C:/hadoop")):
            if (candidate / "bin" / "winutils.exe").exists():
                _os.environ["HADOOP_HOME"] = str(candidate)
                _os.environ["PATH"] = str(candidate / "bin") + _os.pathsep + _os.environ.get("PATH", "")
                break


if _sys.platform == "win32":
    _ensure_spark_env()
# -----------------------------------------------------------------------------

"""
Iceberg sink for the realtime voting pipeline.

Reads the same "votes_topic" as spark-streaming.py, but instead of only publishing
aggregates back to Kafka it persists:

  1. votes_raw            - every vote event, append-only (the immutable fact table)
  2. votes_per_candidate  - running totals per candidate (MERGE / upsert)
  3. turnout_by_location  - running totals per voter state (MERGE / upsert)

Why Iceberg and not plain Parquet:
  - MERGE INTO lets the stream upsert aggregates instead of appending duplicates
  - schema evolution: add a column without rewriting historical files
  - time travel: query the table as of an earlier snapshot when investigating a dispute

Run (Windows, Spark local mode):
    python iceberg_sink.py

Requirements:
    pip install pyspark==3.5.0
    Kafka broker reachable at localhost:9092 with votes_topic already populated
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as _sum, count as _count, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
)

HERE = Path(__file__).resolve().parent
WAREHOUSE = HERE / "iceberg_warehouse"
CHECKPOINT = HERE / "checkpoints"

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "votes_topic"

# Versions must match the Spark minor version you run locally (3.5.x -> _2.12 / 3.5.1).
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ElectionIcebergSink")
        .master("local[*]")
        .config("spark.jars.packages", f"{ICEBERG_RUNTIME},{KAFKA_CONNECTOR}")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE.as_uri())
        # keep the same setting as spark-streaming.py so behaviour is comparable
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def create_tables(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS local.voting")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.voting.votes_raw (
            voter_id            STRING,
            candidate_id        STRING,
            voting_time         TIMESTAMP,
            voter_name          STRING,
            party_affiliation   STRING,
            candidate_name      STRING,
            gender              STRING,
            nationality         STRING,
            registered_age      INT,
            state               STRING,
            city                STRING,
            vote                INT,
            ingested_at         TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(voting_time))
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.voting.votes_per_candidate (
            candidate_id        STRING,
            candidate_name      STRING,
            party_affiliation   STRING,
            total_votes         BIGINT
        )
        USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.voting.turnout_by_location (
            state               STRING,
            total_votes         BIGINT
        )
        USING iceberg
        """
    )


# Same payload contract as spark-streaming.py, so both jobs can run side by side.
VOTE_SCHEMA = StructType([
    StructField("voter_id", StringType(), True),
    StructField("candidate_id", StringType(), True),
    StructField("voting_time", TimestampType(), True),
    StructField("voter_name", StringType(), True),
    StructField("party_affiliation", StringType(), True),
    StructField("biography", StringType(), True),
    StructField("campaign_platform", StringType(), True),
    StructField("photo_url", StringType(), True),
    StructField("candidate_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("registration_number", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", StringType(), True),
    ]), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("cell_number", StringType(), True),
    StructField("picture", StringType(), True),
    StructField("registered_age", IntegerType(), True),
    StructField("vote", IntegerType(), True),
])


def foreach_batch(spark: SparkSession):
    """Persist each micro-batch: append facts, upsert aggregates."""

    def _write(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        # 1) facts: append-only
        raw = (
            batch_df.select(
                "voter_id", "candidate_id", "voting_time", "voter_name",
                "party_affiliation", "candidate_name", "gender", "nationality",
                "registered_age", col("address.state").alias("state"),
                col("address.city").alias("city"), "vote",
            )
            .withColumn("ingested_at", current_timestamp())
        )
        raw.writeTo("local.voting.votes_raw").append()

        # 2) aggregates: rebuilt from the fact table and MERGEd.
        # Using a subquery over facts (instead of a temp view on batch_df) keeps the
        # statement self-contained and makes the totals absolute, so replaying a batch
        # cannot double count.
        spark.sql(
            """
            MERGE INTO local.voting.votes_per_candidate AS t
            USING (
                SELECT candidate_id,
                       candidate_name,
                       party_affiliation,
                       SUM(vote) AS total_votes
                FROM local.voting.votes_raw
                GROUP BY candidate_id, candidate_name, party_affiliation
            ) AS s
            ON t.candidate_id = s.candidate_id
            WHEN MATCHED THEN
                UPDATE SET t.total_votes = s.total_votes,
                           t.candidate_name = s.candidate_name,
                           t.party_affiliation = s.party_affiliation
            WHEN NOT MATCHED THEN
                INSERT (candidate_id, candidate_name, party_affiliation, total_votes)
                VALUES (s.candidate_id, s.candidate_name, s.party_affiliation, s.total_votes)
            """
        )

        spark.sql(
            """
            MERGE INTO local.voting.turnout_by_location AS t
            USING (
                SELECT state, COUNT(*) AS total_votes
                FROM local.voting.votes_raw
                GROUP BY state
            ) AS s
            ON t.state = s.state
            WHEN MATCHED THEN
                UPDATE SET t.total_votes = s.total_votes
            WHEN NOT MATCHED THEN
                INSERT (state, total_votes) VALUES (s.state, s.total_votes)
            """
        )

        print(f"[batch {batch_id}] facts appended, aggregates merged")

    return _write


def main() -> None:
    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    CHECKPOINT.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)

    votes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
        .select(from_json(col("value"), VOTE_SCHEMA).alias("data"))
        .select("data.*")
        .withWatermark("voting_time", "1 minute")
    )

    query = (
        votes.writeStream
        .foreachBatch(foreach_batch(spark))
        .option("checkpointLocation", str(CHECKPOINT / "iceberg_sink"))
        .outputMode("append")
        .start()
    )

    print("Iceberg sink running. Query the tables with iceberg_demo.py in another shell.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
