"""Enriches streaming events with slow-changing historical/reference data
read from S3 (the "processed" or reference layer), e.g. sector, exchange,
52-week average. Loaded once per job as a small static DataFrame and
broadcast-joined so every micro-batch avoids a shuffle.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

REFERENCE_SCHEMA_COLUMNS = ["symbol", "sector", "exchange", "avg_52w_price"]


def load_reference_data(spark: SparkSession, reference_path: str) -> DataFrame:
    """Reads the static reference table (small - safe to broadcast).

    Expected at e.g. s3a://<raw-bucket>/reference/symbols.csv with columns
    matching REFERENCE_SCHEMA_COLUMNS. Falls back to an empty, correctly
    typed DataFrame if the path doesn't exist yet, so a fresh environment
    doesn't hard-fail before `aws/bootstrap_aws.py` has seeded the data.
    """
    try:
        return spark.read.option("header", "true").csv(reference_path)
    except Exception:
        return spark.createDataFrame([], schema=",".join(f"{c} STRING" for c in REFERENCE_SCHEMA_COLUMNS))


def enrich_with_reference(events_df: DataFrame, reference_df: DataFrame) -> DataFrame:
    """Broadcast-join streaming events with the static reference table on
    `symbol`. Broadcasting avoids a shuffle on every micro-batch - safe
    here because the reference table is tiny (one row per symbol/sensor).
    """
    return events_df.join(F.broadcast(reference_df), on="symbol", how="left")
