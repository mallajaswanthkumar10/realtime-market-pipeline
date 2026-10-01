import time

import pytest

from utils.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState


def test_starts_closed():
    cb = CircuitBreaker()
    assert cb.state == CircuitState.CLOSED


def test_opens_after_threshold_failures():
    cb = CircuitBreaker(failure_threshold=3, reset_timeout_seconds=60)

    def fail():
        raise RuntimeError("boom")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            cb.call(fail)

    assert cb.state == CircuitState.OPEN


def test_open_circuit_fails_fast_without_calling_function():
    cb = CircuitBreaker(failure_threshold=1, reset_timeout_seconds=60)
    calls = {"n": 0}

    def fail():
        calls["n"] += 1
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        cb.call(fail)

    with pytest.raises(CircuitOpenError):
        cb.call(fail)

    assert calls["n"] == 1  # second call never reached the function


def test_half_open_after_reset_timeout_then_closes_on_success():
    cb = CircuitBreaker(failure_threshold=1, reset_timeout_seconds=0.05)

    with pytest.raises(RuntimeError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    assert cb.state == CircuitState.OPEN
    time.sleep(0.06)
    assert cb.state == CircuitState.HALF_OPEN

    result = cb.call(lambda: "ok")
    assert result == "ok"
    assert cb.state == CircuitState.CLOSED


def test_success_resets_failure_count():
    cb = CircuitBreaker(failure_threshold=2, reset_timeout_seconds=60)

    with pytest.raises(RuntimeError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    assert cb.call(lambda: "ok") == "ok"
    assert cb.state == CircuitState.CLOSED
