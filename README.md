# Real-Time Market Data Pipeline

A real-time streaming data pipeline that ingests simulated stock-price (or
IoT sensor) events through **Kafka**, processes them with **PySpark
Structured Streaming** (5-minute tumbling-window aggregation, z-score
anomaly detection, S3 enrichment), lands the results in an **S3 data lake**
catalogued by **AWS Glue** and queryable via **Athena**, and visualizes
live metrics on a **Streamlit dashboard** — with dead-letter queues, retry
logic, and a circuit breaker protecting the alerting path.

The entire stack runs locally for free via Docker Compose, using
[LocalStack](https://localstack.cloud/) to emulate S3/Glue/Athena, so you
can run and demo the whole thing without an AWS account. The same code
points at real AWS by changing one environment variable.

```mermaid
flowchart LR
    subgraph Producers
        P1[stock_producer.py]
        P2[iot_producer.py]
    end

    subgraph Kafka
        T1[(stock.raw)]
        T2[(iot.raw)]
        T3[(alerts)]
        T4[(alerts.dlq)]
    end

    subgraph Spark["PySpark Structured Streaming"]
        W["5-min tumbling window
        aggregation"]
        A["Anomaly detection
        (z-score > 2std)"]
        E["Enrichment
        (broadcast join w/ S3 ref data)"]
    end

    subgraph AWS["S3 / Glue / Athena (LocalStack or real AWS)"]
        S3R[(S3 raw)]
        S3P[(S3 processed)]
        S3M[(S3 mart)]
        Glue[Glue Data Catalog]
        Athena[Athena SQL]
    end

    subgraph Consumers
        AC[alert_consumer.py
        retry + circuit breaker]
        D[Streamlit dashboard]
    end

    P1 --> T1
    P2 --> T2
    T1 --> Spark
    S3R -. reference data .-> E
    W --> S3M
    E --> S3P
    A --> T3
    T3 --> AC
    AC -- repeated failure --> T4
    T4 -. replay.py .-> T3
    S3P --> Glue --> Athena
    S3M --> Glue
    T1 --> D
    T3 --> D
```

## Why this exists

This is a portfolio project demonstrating an end-to-end streaming data
engineering stack: ingestion, stateful stream processing, a lakehouse
layout, SQL-on-S3, observability, and the resilience patterns (DLQ,
retry, circuit breaker, idempotent writes) that separate a toy pipeline
from one that would survive production traffic.

## Feature → implementation map

| Requirement | Where it lives |
|---|---|
| Real-time stock/IoT event simulation | [`producer/stock_producer.py`](producer/stock_producer.py), [`producer/iot_producer.py`](producer/iot_producer.py) |
| Kafka topics: raw / enriched / alerts | [`infra/create_topics.sh`](infra/create_topics.sh), [`utils/config.py`](utils/config.py) |
| 5-minute tumbling window aggregation | [`streaming/spark_streaming_job.py`](streaming/spark_streaming_job.py) (`start_windowed_aggregates_sink`) |
| Anomaly detection (\|price - mean\| > 2σ) | [`streaming/anomaly_math.py`](streaming/anomaly_math.py) (pure, unit-tested) + [`streaming/anomaly_detection.py`](streaming/anomaly_detection.py) (Spark transform) |
| Historical enrichment from S3 | [`streaming/enrichment.py`](streaming/enrichment.py) (broadcast join) |
| Write partitioned by date/hour | `partitionBy("date", "hour")` in `spark_streaming_job.py` |
| S3 raw / processed / mart buckets | [`aws/bootstrap_aws.py`](aws/bootstrap_aws.py) |
| Glue Data Catalog | `aws/bootstrap_aws.py::create_glue_tables` |
| Athena SQL queries | [`aws/athena_queries.sql`](aws/athena_queries.sql), [`aws/run_athena_query.py`](aws/run_athena_query.py) |
| Real-time dashboard | [`dashboard/app.py`](dashboard/app.py) (Streamlit, subscribes directly to Kafka) |
| Dead-letter queue | `alerts.dlq` topic, routed from [`consumer/alert_consumer.py`](consumer/alert_consumer.py) |
| Retry logic | [`utils/retry.py`](utils/retry.py) (exponential backoff + jitter) |
| Circuit breaker | [`utils/circuit_breaker.py`](utils/circuit_breaker.py) |
| Idempotent / exactly-once writes | batch-id-keyed deterministic S3 paths in `spark_streaming_job.py` — see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#exactly-once--idempotent-writes) |
| 7-day replay | Kafka `retention.ms` in `infra/create_topics.sh` + [`consumer/replay.py`](consumer/replay.py) |
| Auto-scaling consumers | Kafka consumer groups + Spark `dynamicAllocation` config — see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#scaling-to-10000-msgsec) |
| Zero-downtime deploys | Rolling restart notes — see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#zero-downtime-deployments) |
| Data quality checks | Schema validation + null-filtering in `read_raw_stream()` |
| Cost optimization | Batch vs. streaming tradeoffs — see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#cost-optimization-batch-vs-streaming) |

Full design rationale, scaling, and cost discussion: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Prerequisites

- Docker + Docker Compose (Kafka, Kafka UI, LocalStack)
- Python 3.10+ with `pip`
- Java 11 or 17 **only** if you want to run the PySpark streaming job (`pyspark` needs a JVM). Everything else (producers, dashboard, AWS bootstrap, tests) is pure Python.

## Quickstart

```bash
# 1. Start Kafka + LocalStack
docker compose up -d

# 2. Install Python dependencies
python -m venv .venv && source .venv/bin/activate   # .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env

# 3. Create Kafka topics (6 partitions, 7-day retention)
docker exec -it pipeline-kafka bash -c "$(cat infra/create_topics.sh)"

# 4. Create S3 buckets + Glue catalog (against LocalStack by default)
python -m aws.bootstrap_aws

# 5. Start producing simulated stock ticks
python -m producer.stock_producer --rate 20

# 6. In another terminal: run the Spark streaming job (needs Java)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  streaming/spark_streaming_job.py

# 7. In another terminal: run the alert consumer (DLQ + retry + circuit breaker)
python -m consumer.alert_consumer

# 8. In another terminal: launch the live dashboard
streamlit run dashboard/app.py
```

Kafka UI is available at `http://localhost:8080` to inspect topics/messages directly.

## Running the tests

The resilience and anomaly-detection logic is pure Python and unit tested
without needing Kafka, Spark, or AWS running:

```bash
pip install pytest
pytest tests/ -v
```

## Project layout

```
producer/     simulated stock + IoT event generators -> Kafka
streaming/    PySpark Structured Streaming job, anomaly math, enrichment
consumer/     alert delivery (retry + circuit breaker + DLQ), replay tooling
aws/          S3 / Glue / Athena bootstrap and query helpers (boto3)
dashboard/    Streamlit real-time visualization
utils/        shared config, Kafka client factory, circuit breaker, retry
infra/        Kafka topic provisioning
tests/        unit tests for anomaly math, circuit breaker, retry
docs/         architecture, scaling, and cost-optimization notes
```

## Limitations / what's simulated

- **Data source**: ticks are synthetically generated (random walk + injected
  spikes) rather than pulled from a live market feed, so the pipeline is
  self-contained and demoable offline.
- **LocalStack Athena**: query *execution* against Parquet in LocalStack's
  free tier is limited; `aws/run_athena_query.py` is written against the
  real Athena API and works as-is once pointed at an actual AWS account
  (remove `AWS_ENDPOINT_URL`). For local querying, read the same Parquet
  output directly with `spark.read.parquet(...)` or DuckDB.
- **Throughput claims**: the "10,000+ msg/sec" and "auto-scaling" targets
  describe the architecture's design point (partition count, consumer
  group sizing, Spark dynamic allocation) rather than a benchmark run in
  this repo — see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#scaling-to-10000-msgsec)
  for how it gets there and what you'd need to measure to confirm it.

## License

MIT — see [LICENSE](LICENSE).
