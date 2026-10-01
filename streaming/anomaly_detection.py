"""Spark DataFrame transform for anomaly tagging.

The actual z-score math lives in anomaly_math.py (no PySpark dependency,
unit tested directly). This module just applies the same threshold logic
as a Spark expression over a streaming micro-batch.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.anomaly_math import zscore, is_anomalous  # re-exported for convenience
from utils import config

__all__ = ["zscore", "is_anomalous", "tag_anomalies_df"]


def tag_anomalies_df(events_df: DataFrame, window_stats_df: DataFrame) -> DataFrame:
    """Join raw events against their window's (mean, stddev) and flag
    anomalies where |price - mean| > threshold * stddev.

    `events_df` must have columns: event_id, symbol, price, event_time, window
    `window_stats_df` must have columns: symbol, window, mean_price, stddev_price
    """
    threshold = config.ANOMALY_STDDEV_THRESHOLD

    joined = events_df.join(window_stats_df, on=["symbol", "window"], how="left")

    with_z = joined.withColumn(
        "zscore",
        F.when(
            (F.col("stddev_price").isNotNull()) & (F.col("stddev_price") > 0),
            (F.col("price") - F.col("mean_price")) / F.col("stddev_price"),
        ).otherwise(F.lit(None).cast("double")),
    )

    return with_z.withColumn(
        "is_anomaly",
        F.when(F.abs(F.col("zscore")) > F.lit(threshold), F.lit(True)).otherwise(F.lit(False)),
    )
