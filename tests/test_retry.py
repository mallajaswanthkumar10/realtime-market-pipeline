import pytest

from utils.retry import retry_with_backoff


def test_succeeds_without_retry():
    calls = {"n": 0}

    @retry_with_backoff(max_attempts=3, sleep_fn=lambda s: None)
    def always_ok():
        calls["n"] += 1
        return "ok"

    assert always_ok() == "ok"
    assert calls["n"] == 1


def test_retries_then_succeeds():
    calls = {"n": 0}

    @retry_with_backoff(max_attempts=5, base_delay=0.01, sleep_fn=lambda s: None)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_exhausts_attempts_and_raises_last_exception():
    calls = {"n": 0}

    @retry_with_backoff(max_attempts=3, base_delay=0.01, sleep_fn=lambda s: None)
    def always_fails():
        calls["n"] += 1
        raise ValueError(f"fail-{calls['n']}")

    with pytest.raises(ValueError, match="fail-3"):
        always_fails()
    assert calls["n"] == 3


def test_sleep_called_with_increasing_delay():
    sleeps = []

    @retry_with_backoff(max_attempts=4, base_delay=1.0, jitter=0.0, sleep_fn=sleeps.append)
    def always_fails():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        always_fails()

    assert sleeps == [1.0, 2.0, 4.0]


def test_only_retries_specified_exceptions():
    @retry_with_backoff(max_attempts=3, sleep_fn=lambda s: None, exceptions=(ValueError,))
    def raises_type_error():
        raise TypeError("not retried")

    with pytest.raises(TypeError):
        raises_type_error()
