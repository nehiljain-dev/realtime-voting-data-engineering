# Iceberg sink — extension to the realtime voting pipeline

This folder adds a **lakehouse sink** to the existing voting pipeline. Instead of
publishing aggregates only to Kafka, the votes are persisted in **Apache Iceberg**
tables, which gives three things plain Parquet cannot:

| Requirement | How Iceberg solves it here |
|---|---|
| Upsert aggregates without duplicates | `MERGE INTO` on `votes_per_candidate` / `turnout_by_location` |
| Change schema without rewriting history | `ALTER TABLE ... ADD COLUMN region` keeps old files valid |
| Audit "what did the counts look like at 14:05?" | `VERSION AS OF <snapshot_id>` |

## Files

| File | Purpose |
|---|---|
| `iceberg_sink.py` | Structured Streaming job: `votes_topic` → Iceberg facts + upserted aggregates |
| `iceberg_demo.py` | Read the tables, show snapshots, add columns, time travel, consistency check |

## Prerequisites

```bash
pip install pyspark==3.5.0
```

The existing `docker-compose.yml` must be up (Kafka on `localhost:9092`, PostgreSQL on
`localhost:5432`), and `votes_topic` must be receiving events (run `voting.py`).

## Run

```bash
# terminal 1 - the existing producer
python voting.py

# terminal 2 - the Iceberg sink
cd iceberg
python iceberg_sink.py

# terminal 3 - after a few micro-batches, inspect
cd iceberg
python iceberg_demo.py
```

The warehouse is created at `iceberg/iceberg_warehouse/`, checkpoints at
`iceberg/checkpoints/` — both local, nothing to install.

## Schema evolution demo

```sql
ALTER TABLE local.voting.votes_raw ADD COLUMN region STRING;
DESCRIBE local.voting.votes_raw;
```

Old data files are untouched: the new column reads as `NULL` for older snapshots, and new
writes can populate it. No `INSERT OVERWRITE`, no downtime.

## Time travel demo

```sql
SELECT snapshot_id, committed_at, operation FROM local.voting.votes_raw.snapshots;

SELECT COUNT(*) FROM local.voting.votes_raw VERSION AS OF <snapshot_id>;
```

Useful in a dispute: "the dashboard showed 41% at 14:05, prove it" — the snapshot exists.

## What changed vs the original pipeline

1. **Hardcoded macOS paths removed.** The original `spark-streaming.py` points at
   `/Users/airscholar/...` for the PostgreSQL JDBC jar and for checkpoints; this job
   resolves paths relative to the project so it runs on any machine.
2. **Facts and aggregates separated.** The original only emits aggregates to Kafka, which
   means the raw vote event is lost once the window closes. Here the fact table is
   append-only and the aggregates are derived from it, so any future metric can be
   recomputed.
3. **Idempotent aggregates.** `MERGE INTO` replaces `outputMode("update")` to Kafka, so a
   restarted job does not double-count.

## Interview notes

- **Why not just Parquet?** No `MERGE`, no schema evolution without rewriting, no snapshots.
- **Why `foreachBatch`?** `MERGE INTO` is a table operation, not a row-level writer; `foreachBatch` gives idempotent table-level writes.
- **What about exactly-once?** Checkpoint plus `MERGE` keyed on `candidate_id` / `state` makes replays safe; the fact table would need a dedup key (e.g. `voter_id + voting_time`) for strict exactly-once.
- **Partitioning:** `PARTITIONED BY (days(voting_time))` so a day's votes can be pruned on read; `votes_raw.partitions` shows files per partition.
- **Limitation:** the Hadoop catalog is local-only. In production this would be a REST
  catalog (Nessie/Unity/Polaris) with object storage, so several writers can commit safely.
