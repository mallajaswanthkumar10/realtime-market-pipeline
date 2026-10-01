"""PySpark schemas for the raw event payloads produced by producer/*.

Kept as an explicit StructType (rather than schema inference) so the
streaming job fails fast on a malformed payload instead of silently
producing nulls, and so the Kafka JSON parsing step is testable without a
running cluster.
"""
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("volume", DoubleType(), nullable=True),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("source", StringType(), nullable=True),
    ]
)

ALERT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("window_mean", DoubleType(), nullable=False),
        StructField("window_stddev", DoubleType(), nullable=False),
        StructField("zscore", DoubleType(), nullable=False),
        StructField("event_time", StringType(), nullable=False),
        StructField("alert_type", StringType(), nullable=False),
    ]
)
