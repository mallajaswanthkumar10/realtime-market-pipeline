"""Pure-Python anomaly math - deliberately has zero PySpark dependency so
it can be unit tested in milliseconds without a Spark session/JVM. See
anomaly_detection.py for the Spark DataFrame transform that applies this
same logic to a streaming batch.
"""
from __future__ import annotations

from typing import Optional


def zscore(value: float, mean: float, stddev: float) -> Optional[float]:
    """Standard score of `value` against a window's mean/stddev.

    Returns None when stddev is 0/undefined (e.g. a window with a single
    sample) since "deviation" is meaningless there - callers should treat
    None as "not enough data to judge".
    """
    if stddev is None or stddev == 0:
        return None
    return (value - mean) / stddev


def is_anomalous(value: float, mean: float, stddev: float, threshold: float = 2.0) -> bool:
    z = zscore(value, mean, stddev)
    if z is None:
        return False
    return abs(z) > threshold
