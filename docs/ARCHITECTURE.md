# Architecture Notes

## Data flow

1. **Producers** (`producer/stock_producer.py`, `producer/iot_producer.py`)
   simulate per-symbol/per-sensor events and publish to Kafka, keyed by
   `symbol`/`sensor_id` so all events for a given key land on the same
   partition — this preserves per-key ordering, which the windowed
   aggregation depends on.

2. **Kafka** holds three logical stages as separate topics:
   - `stock.raw` / `iot.raw` — raw ingested events (6 partitions, 7-day retention)
   - `alerts` — anomalies emitted by the streaming job (3 partitions)
   - `alerts.dlq` — alerts that failed delivery after retries (1 partition, low volume by design)

3. **PySpark Structured Streaming** (`streaming/spark_streaming_job.py`) runs
   two streaming queries off the same source:
   - **Query A** — a stateful `groupBy(window(event_time, "5 minutes"), symbol)`
     aggregation (mean/stddev/min/max/count), written continuously to the
     S3 **mart** layer, partitioned by `date`/`hour`.
   - **Query B** — a `foreachBatch` sink that, per micro-batch: computes
     batch-local window stats, joins raw events against them to compute a
     z-score, broadcast-joins against static reference data loaded once
     from S3, writes the enriched+tagged rows to the S3 **processed**
     layer, and publishes rows where `|zscore| > 2` to the `alerts` topic.

4. **S3 / Glue / Athena**: `aws/bootstrap_aws.py` creates three buckets
   (raw/processed/mart), seeds `reference/symbols.csv` into raw, and
   registers Glue tables over the processed and mart prefixes so Athena
   can run SQL directly against the partitioned Parquet output
   (`aws/athena_queries.sql`).

5. **`consumer/alert_consumer.py`** consumes `alerts`, attempts delivery
   to a downstream sink (stubbed as a log line — swap for a real
   webhook), wrapped in retry-with-backoff + a circuit breaker. Anything
   that still fails goes to `alerts.dlq` with failure context instead of
   being dropped. `consumer/replay.py` can replay the DLQ back onto
   `alerts`, or seek a fresh consumer group to any point in the last 7
   days to backfill from raw.

6. **`dashboard/app.py`** subscribes directly to `stock.raw` and `alerts`
   and redraws live charts/metrics — no intermediate API layer.

## Exactly-once / idempotent writes

Kafka → Spark → S3 pipelines can't rely on a single end-to-end
transaction, so exactly-once here is achieved per-hop:

- **Kafka → Spark**: Structured Streaming's checkpointing records exactly
  which offsets were included in each committed micro-batch. On restart
  after a failure, Spark re-reads the same offset range for the
  in-flight batch — it will never skip or silently duplicate at the
  *source* read.
- **Spark → S3 (the hard part)**: S3 isn't transactional, so a batch that
  fails partway through a write could, naively, leave partial data and
  then duplicate it on retry. We avoid this in `make_batch_processor()` by
  naming each batch's output path deterministically with Spark's
  `batch_id` (`.../batch_id={batch_id}`) and writing with `mode="overwrite"`
  scoped to that path. Spark guarantees `batch_id` is stable across
  retries of the same micro-batch (given a replayable source like Kafka),
  so re-running a failed batch overwrites exactly its own prior (possibly
  partial) output rather than appending a duplicate. This is the standard
  recipe Spark's own docs recommend for sinks that aren't natively
  idempotent.
- **Spark → Kafka (alerts)**: the `alerts` topic is a notification
  stream, not a system of record — the small risk of a duplicate alert
  on a retried batch is intentionally accepted (false-positive-safe),
  versus the asymmetric cost of ever *losing* an anomaly alert.
- **Downstream delivery**: `alert_consumer.py` commits its Kafka offset
  only after the alert reaches a terminal state (delivered, or routed to
  DLQ) — at-least-once delivery that's effectively-once because `notify()`
  is keyed by `event_id` and is safe to call twice.

## Scaling to 10,000+ msg/sec

- **Partitioning**: `stock.raw` is created with 6 partitions
  (`infra/create_topics.sh`); partition count is the hard ceiling on
  consumer parallelism, so it's sized to the number of concurrent Spark
  tasks/consumer instances you expect to run, not to current load.
  Repartitioning up is possible later but never down, so start higher
  than day-one traffic needs.
- **Producer-side**: `acks="all"` + batching (`linger_ms`) trades a small
  amount of latency for much higher throughput per producer by batching
  small messages into fewer, larger requests — the standard Kafka
  throughput lever.
- **Spark auto-scaling**: `spark.dynamicAllocation.enabled=true` (set in
  `build_spark_session()`) lets the cluster add executors as the
  micro-batch backlog grows and release them when load drops, rather than
  provisioning for peak 24/7.
- **Consumer-side**: `alert_consumer.py` and the dashboard each use a
  Kafka consumer group — running multiple instances of a consumer group
  member lets Kafka rebalance partitions across them automatically; this
  is how you scale alert-delivery throughput horizontally without code
  changes.
- **What would need to change for a real 10k/sec benchmark**: a multi-broker
  Kafka cluster (this repo's Docker Compose is a single broker for local
  dev), a real multi-node Spark cluster (EMR/Databricks) instead of local
  mode, and a load generator driving `producer/stock_producer.py`'s
  `--rate` far beyond single-process Python throughput (at that point
  you'd run multiple producer processes, which the topic's partitioning
  already supports).

## Zero-downtime deployments

- **Streaming job**: Spark Structured Streaming checkpoints offsets and
  state to the `checkpointLocation`. Deploying a new job version is a
  rolling restart: stop the old query gracefully (`query.stop()` after
  the current micro-batch completes), deploy the new jar/script pointed
  at the *same* checkpoint directory, and it resumes exactly where the
  old one left off — no gap, no reprocessing (as long as the change
  doesn't alter the aggregation's schema/state format, which needs a new
  checkpoint directory instead).
- **Consumers** (`alert_consumer.py`, the dashboard): these are stateless
  Kafka consumer group members. Rolling one instance at a time (start the
  new version, wait for it to join the group, then stop one old
  instance, repeat) means the group never drops to zero members, so no
  message processing window is lost — this is the same pattern
  Kubernetes `RollingUpdate` deployments automate.

## Cost optimization: batch vs. streaming

- **Streaming is the right call here** because the explicit requirement is
  real-time anomaly alerting — a 2-standard-deviation price spike is only
  actionable within minutes, not hours.
- **But not everything needs to be streaming**: the reference/enrichment
  data (`data/reference/symbols.csv`) changes rarely, so it's loaded once
  per job as a small batch read and broadcast — paying the S3 read cost
  once per job restart instead of once per event.
- **Trigger interval as a cost lever**: `trigger(processingTime="10 seconds")`
  / `"30 seconds"` in `spark_streaming_job.py` batches several seconds of
  events per micro-batch. Shorter intervals lower latency but increase
  per-batch overhead (more, smaller S3 writes — worse for both cost and
  Parquet file-size health); longer intervals do the opposite. 10–30s is
  a reasonable default for "real-time" alerting without generating a
  small-files problem in S3.
- **Partitioning by date/hour** keeps Athena queries cost-proportional to
  the time range actually queried (Athena bills by bytes scanned) instead
  of forcing a full-table scan for "what happened in the last hour."
- **S3 storage classes**: the mart layer (aggregates, queried often,
  small) is a good `S3 Standard` candidate; the processed layer (raw
  enriched events, queried rarely after the fact) is a good candidate for
  a lifecycle rule transitioning to `S3 Infrequent Access` or `Glacier
  Instant Retrieval` after N days — not implemented here, but a one-line
  bucket lifecycle policy addition to `aws/bootstrap_aws.py`.

## Data quality

`read_raw_stream()` in `spark_streaming_job.py` parses incoming JSON
against an explicit `StructType` (`streaming/schemas.py`) rather than
inferring it, so a malformed payload produces nulls instead of crashing
the job, and rows missing `event_id`/`event_time` are filtered out before
they can pollute a window aggregate. For a production system, you'd add:
range checks (e.g. reject negative prices), duplicate `event_id` detection
within a window, and a metrics counter for rows dropped by each check so
data-quality regressions are visible on a dashboard rather than silent.

## Deploying to real AWS

Everything in `aws/` and `streaming/spark_streaming_job.py` talks to AWS
through `boto3`/`hadoop-aws`'s standard S3 API — swapping LocalStack for
real AWS is: remove `AWS_ENDPOINT_URL` from `.env`, provide real AWS
credentials (env vars, instance role, or `~/.aws/credentials`), and run
the same scripts. For production you'd additionally:

- Run Kafka as **Amazon MSK** instead of the single-broker Docker Compose setup.
- Run the Spark job on **EMR** or **Databricks** instead of local `spark-submit`.
- Replace the single `data/reference/symbols.csv` seed with a real
  reference-data pipeline (e.g. a daily batch job refreshing it in S3).
- Add a **Glue Crawler** on a schedule instead of manually running
  `MSCK REPAIR TABLE` to discover new partitions.
