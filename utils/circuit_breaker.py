"""A minimal, dependency-free circuit breaker.

Protects downstream calls (e.g. an alert webhook, a DB write) from being
hammered while the downstream is unhealthy. Three states:

    CLOSED     -> calls go through normally.
    OPEN       -> calls are rejected immediately (fail fast) until the
                  reset timeout elapses.
    HALF_OPEN  -> a single trial call is let through; success closes the
                  breaker again, failure re-opens it.
"""
from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Callable, TypeVar

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(RuntimeError):
    """Raised when a call is rejected because the breaker is OPEN."""


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout_seconds: float = 30.0,
        name: str = "default",
    ) -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout_seconds = reset_timeout_seconds
        self.name = name

        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _maybe_half_open(self) -> None:
        if (
            self._state is CircuitState.OPEN
            and self._opened_at is not None
            and (time.monotonic() - self._opened_at) >= self.reset_timeout_seconds
        ):
            self._state = CircuitState.HALF_OPEN

    def _on_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._state = CircuitState.CLOSED
            self._opened_at = None

    def _on_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            if (
                self._state is CircuitState.HALF_OPEN
                or self._failure_count >= self.failure_threshold
            ):
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        with self._lock:
            self._maybe_half_open()
            if self._state is CircuitState.OPEN:
                raise CircuitOpenError(
                    f"circuit '{self.name}' is OPEN - failing fast"
                )

        try:
            result = func(*args, **kwargs)
        except Exception:
            self._on_failure()
            raise
        else:
            self._on_success()
            return result
