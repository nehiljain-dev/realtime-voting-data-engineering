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
Query and demonstrate the Iceberg features that justify using Iceberg
instead of plain Parquet.

Run after iceberg_sink.py has written at least one micro-batch:

    python iceberg_demo.py

Sections:
  1. Read the tables
  2. Snapshots (history of the table)
  3. Schema evolution - add a column without rewriting data
  4. Time travel - query an earlier snapshot
  5. Incremental-style check: compare totals
"""
from pathlib import Path

from pyspark.sql import SparkSession

HERE = Path(__file__).resolve().parent
WAREHOUSE = HERE / "iceberg_warehouse"

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("IcebergDemo")
        .master("local[*]")
        .config("spark.jars.packages", ICEBERG_RUNTIME)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE.as_uri())
        .getOrCreate()
    )


def show(spark, title: str, sql: str) -> None:
    print(f"\n=== {title} ===")
    print(sql.strip())
    spark.sql(sql).show(truncate=False)


def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    show(spark, "Candidates", "SELECT * FROM local.voting.votes_per_candidate ORDER BY total_votes DESC")
    show(spark, "Turnout by location", "SELECT * FROM local.voting.turnout_by_location ORDER BY total_votes DESC")
    show(spark, "Table snapshots", "SELECT snapshot_id, committed_at, operation FROM local.voting.votes_raw.snapshots")

    # --- schema evolution: add columns to an existing table without rewriting files
    spark.sql("ALTER TABLE local.voting.votes_raw ADD COLUMN region STRING")
    spark.sql("ALTER TABLE local.voting.votes_raw ADD COLUMN device_type STRING")
    show(spark, "Schema after ALTER", "DESCRIBE local.voting.votes_raw")

    # --- time travel: read the first snapshot (before the new columns existed)
    history = spark.sql(
        "SELECT snapshot_id FROM local.voting.votes_raw.snapshots ORDER BY committed_at ASC"
    ).collect()
    if history:
        first_snapshot = history[0]["snapshot_id"]
        show(
            spark,
            f"Time travel to first snapshot ({first_snapshot})",
            f"SELECT COUNT(*) AS votes_at_first_snapshot FROM local.voting.votes_raw VERSION AS OF {first_snapshot}",
        )
        show(
            spark,
            "Latest snapshot",
            "SELECT COUNT(*) AS votes_now FROM local.voting.votes_raw",
        )

    # --- sanity check: aggregates should equal facts
    show(
        spark,
        "Consistency check (facts vs aggregates)",
        """
        SELECT
            (SELECT SUM(vote) FROM local.voting.votes_raw)                        AS facts_total,
            (SELECT SUM(total_votes) FROM local.voting.votes_per_candidate)        AS aggregate_total
        """,
    )

    # --- partition pruning evidence
    show(
        spark,
        "Partitions written",
        "SELECT partition, record_count, file_count FROM local.voting.votes_raw.partitions",
    )


if __name__ == "__main__":
    main()
