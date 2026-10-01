"""Central config loader. Every setting is overridable via env vars / .env."""
import os

from dotenv import load_dotenv

load_dotenv()


def _get(name: str, default: str) -> str:
    return os.environ.get(name, default)


# Kafka
KAFKA_BOOTSTRAP_SERVERS = _get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_STOCK_RAW = _get("TOPIC_STOCK_RAW", "stock.raw")
TOPIC_IOT_RAW = _get("TOPIC_IOT_RAW", "iot.raw")
TOPIC_ALERTS = _get("TOPIC_ALERTS", "alerts")
TOPIC_ALERTS_DLQ = _get("TOPIC_ALERTS_DLQ", "alerts.dlq")
KAFKA_TOPIC_PARTITIONS = int(_get("KAFKA_TOPIC_PARTITIONS", "6"))
KAFKA_RETENTION_MS = int(_get("KAFKA_RETENTION_MS", str(7 * 24 * 60 * 60 * 1000)))

# AWS / LocalStack
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL") or None  # None -> real AWS
AWS_REGION = _get("AWS_REGION", "us-east-1")

S3_BUCKET_RAW = _get("S3_BUCKET_RAW", "market-pipeline-raw")
S3_BUCKET_PROCESSED = _get("S3_BUCKET_PROCESSED", "market-pipeline-processed")
S3_BUCKET_MART = _get("S3_BUCKET_MART", "market-pipeline-mart")
GLUE_DATABASE = _get("GLUE_DATABASE", "market_pipeline")

# Streaming job
WINDOW_DURATION = _get("WINDOW_DURATION", "5 minutes")
WATERMARK_DELAY = _get("WATERMARK_DELAY", "2 minutes")
ANOMALY_STDDEV_THRESHOLD = float(_get("ANOMALY_STDDEV_THRESHOLD", "2.0"))
CHECKPOINT_LOCATION = _get("CHECKPOINT_LOCATION", "./checkpoints/stock_stream")

# Dashboard
DASHBOARD_REFRESH_SECONDS = int(_get("DASHBOARD_REFRESH_SECONDS", "5"))

# Tickers simulated by the producer
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]
