"""Retry decorator with exponential backoff + jitter.

Thin wrapper kept local (rather than only relying on `tenacity`) so the
retry/backoff/jitter behavior is easy to unit test deterministically by
injecting a fake sleep function.
"""
from __future__ import annotations

import functools
import random
import time
from typing import Callable, Tuple, Type, TypeVar

T = TypeVar("T")


def retry_with_backoff(
    max_attempts: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    jitter: float = 0.1,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    sleep_fn: Callable[[float], None] = time.sleep,
):
    """Retries the wrapped function up to `max_attempts` times.

    Delay grows as base_delay * 2**attempt, capped at max_delay, with up to
    `jitter` fraction of random jitter added to avoid thundering-herd
    retries across many consumer instances.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exc: BaseException | None = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:  # noqa: PERF203
                    last_exc = exc
                    if attempt == max_attempts - 1:
                        break
                    delay = min(base_delay * (2**attempt), max_delay)
                    delay += delay * random.uniform(0, jitter)
                    sleep_fn(delay)
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator
