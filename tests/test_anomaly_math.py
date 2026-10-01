from streaming.anomaly_math import is_anomalous, zscore


def test_zscore_basic():
    assert zscore(110, 100, 5) == 2.0
    assert zscore(90, 100, 5) == -2.0


def test_zscore_undefined_stddev_returns_none():
    assert zscore(100, 100, 0) is None
    assert zscore(100, 100, None) is None


def test_is_anomalous_above_threshold():
    assert is_anomalous(111, 100, 5, threshold=2.0) is True


def test_is_anomalous_within_threshold():
    assert is_anomalous(105, 100, 5, threshold=2.0) is False


def test_is_anomalous_negative_direction():
    assert is_anomalous(89, 100, 5, threshold=2.0) is True


def test_is_anomalous_undefined_stddev_is_never_anomalous():
    assert is_anomalous(1000, 100, 0, threshold=2.0) is False
