# Real-Time Market Data Pipeline

[![CI](https://github.com/mallajaswanthkumar10/realtime-market-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/mallajaswanthkumar10/realtime-market-pipeline/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A demo pipeline that simulates live stock prices, streams them through
**Kafka**, processes them in real time with **PySpark**, saves the
results to **AWS S3** (cataloged with **Glue**, queryable via **Athena**),
and shows everything live on a **dashboard**. It also knows how to handle
failures gracefully instead of silently losing data.

Runs entirely on your own machine for free, using Docker to simulate
Kafka and AWS locally. Point it at a real AWS account later by changing
one setting.

## How data flows through it

```
  stock_producer.py  --->  Kafka: "stock.raw" topic
  (fake price ticks)             |
                                  v
                         PySpark streaming job
                     - groups prices into 5-min windows
                     - flags prices that look abnormal
                     - adds extra reference info
                                  |
                 -----------------------------------
                 |                                 |
                 v                                 v
         AWS S3 (processed +           Kafka: "alerts" topic
         mart buckets, via            (abnormal prices only)
         Glue + Athena for SQL)                |
                                                v
                                   alert_consumer.py
                                 (retries, then gives up
                                  gracefully into a
                                  "dead letter" topic
                                  if delivery keeps failing)

  Meanwhile: dashboard.py watches Kafka directly and
  shows live prices + alerts in your browser.
```

## What's in this project

| What it does | Where to find it |
|---|---|
| Generates fake live stock/IoT data | `producer/` |
| Processes the stream (windowing, anomaly detection, enrichment) | `streaming/` |
| Saves data to the cloud (S3, Glue, Athena) | `aws/` |
| Delivers alerts reliably (retry, circuit breaker, dead-letter queue) | `consumer/` |
| Live dashboard | `dashboard/` |
| Shared helpers (config, Kafka setup) | `utils/` |
| Creates the Kafka topics | `infra/` |
| Automated tests | `tests/` |
| Deeper design notes (scaling, cost, exactly-once writes) | `docs/ARCHITECTURE.md` |

## What you need installed

- **Docker** + Docker Compose — runs Kafka and the AWS simulator
- **Python 3.10+**
- **Java 11 or 17** — only needed to run the PySpark job itself. Everything
  else (producers, dashboard, tests) is plain Python and doesn't need Java.

## Running it step by step

```bash
# 1. Start Kafka + the local AWS simulator
docker compose up -d

# 2. Set up Python
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env

# 3. Create the Kafka topics
docker exec -it pipeline-kafka bash -c "$(cat infra/create_topics.sh)"

# 4. Create the S3 buckets + data catalog
python -m aws.bootstrap_aws

# 5. Start generating fake stock ticks
python -m producer.stock_producer --rate 20

# 6. (new terminal) Run the PySpark streaming job — needs Java
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  streaming/spark_streaming_job.py

# 7. (new terminal) Run the alert consumer
python -m consumer.alert_consumer

# 8. (new terminal) Open the live dashboard
streamlit run dashboard/app.py
```

You can browse the Kafka topics directly at `http://localhost:8080`.

## Running the tests

The core logic (anomaly detection, retry, circuit breaker) is tested
without needing Kafka, Spark, or AWS running at all:

```bash
pip install pytest
pytest tests/ -v
```

## Good to know

- **The data is fake.** Prices are randomly generated (with occasional
  intentional spikes) so the pipeline has something to react to, without
  needing a real market data subscription.
- **Athena queries need real AWS.** The local AWS simulator doesn't fully
  support running Athena SQL queries — that part is written to work
  correctly once you point it at a real AWS account. Locally, you can
  read the same saved files directly with Spark or DuckDB instead.
- **"10,000+ messages/sec" describes the design, not a measured benchmark**
  run on this laptop. The architecture (partition counts, auto-scaling
  settings) is built to support that scale — see `docs/ARCHITECTURE.md`
  for exactly how.

More detail on *why* things are built this way — exactly-once writes,
scaling, zero-downtime deploys, cost tradeoffs — is in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## License

MIT — see [LICENSE](LICENSE).
