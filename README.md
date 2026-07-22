# Refyne Bronze Ingestion

Databricks Spark Structured Streaming pipeline (MongoDB → S3 → Bronze Auto Loader),
plus a non-destructive measurement harness for sizing.

## Files
- `bronze_ingest_consumer1.py` — 24×7 bronze ingestion pipeline. Bounded `availableNow`
  wave-drain with a warm-cluster `loop` mode, failure propagation, robust schema-evolution
  classification, advisory single-writer marker, a greppable `EXTRA_TOPICS` placeholder for
  adding topics, and `shard_count`/`shard_index` widgets for running N parallel jobs over
  disjoint topic subsets.
- `drain_measurement_harness.py` — non-destructive profiler (defaults to listing-only;
  isolated measurement checkpoints) that measures per-collection listing/backlog and projects
  full-drain wall-clock across `max_concurrent_streams ∈ {4,6,8,12,16}`.

## Deploy notes
- Set the Job's `max_concurrent_runs=1` (authoritative single-writer guarantee).
- Select `trigger_mode=loop` on a persistent (warm) classic cluster for 24×7.
- Verify the schema-evolution exception identity against a real Auto Loader stack trace.
- Verified via `py_compile` + static + cross-vendor review only; live streaming behavior
  is unverified — use the harness to get real numbers.
