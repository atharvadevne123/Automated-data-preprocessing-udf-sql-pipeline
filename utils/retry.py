"""Retry decorator for transient failures in I/O and network operations."""

from __future__ import annotations

import functools
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Type

logger = logging.getLogger(__name__)


class MaxRetriesExceeded(Exception):
    """Raised when all retry attempts have been exhausted."""


@dataclass
class RetryConfig:
    """Reusable configuration object for the retry decorator.

    Attributes:
        exceptions: Exception types to catch and retry.
        max_attempts: Maximum total invocations (first attempt + retries).
        delay: Initial delay in seconds between attempts.
        backoff: Multiplier applied to delay after each attempt.
        jitter: Random seconds added to each delay to reduce thundering herd.

    Example::

        cfg = RetryConfig(exceptions=(IOError,), max_attempts=5, delay=0.5, backoff=2.0)

        @cfg.as_decorator()
        def fetch_data(url: str) -> bytes:
            ...
    """

    exceptions: tuple[Type[Exception], ...] = field(default_factory=lambda: (Exception,))
    max_attempts: int = 3
    delay: float = 1.0
    backoff: float = 2.0
    jitter: float = 0.0

    def as_decorator(self) -> Callable:
        """Return a retry decorator configured with this instance's parameters."""
        return retry(
            exceptions=self.exceptions,
            max_attempts=self.max_attempts,
            delay=self.delay,
            backoff=self.backoff,
            jitter=self.jitter,
        )


def retry(
    exceptions: tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    jitter: float = 0.0,
) -> Callable:
    """Decorator that retries a function on specified exceptions.

    Args:
        exceptions: Tuple of exception types to retry on.
        max_attempts: Maximum number of total attempts (including the first).
        delay: Initial delay in seconds between retries.
        backoff: Multiplier applied to delay after each retry.
        jitter: Optional random jitter in seconds added to each delay.

    Returns:
        Decorated callable that retries on failure.

    Raises:
        MaxRetriesExceeded: after all attempts are exhausted.
        Any exception not in *exceptions* is re-raised immediately.

    Example::

        @retry(exceptions=(IOError,), max_attempts=3, delay=0.5)
        def read_remote_file(path: str) -> str:
            ...
    """
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
    if delay < 0:
        raise ValueError(f"delay must be >= 0, got {delay}")

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        break
                    wait = current_delay
                    if jitter > 0:
                        import random

                        wait += random.uniform(0, jitter)
                    logger.warning(
                        "Attempt %d/%d failed for %s: %s — retrying in %.2fs",
                        attempt,
                        max_attempts,
                        func.__name__,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                    current_delay *= backoff
            raise MaxRetriesExceeded(
                f"{func.__name__} failed after {max_attempts} attempt(s). Last error: {last_exc}"
            ) from last_exc

        return wrapper

    return decorator


def retry_on_error(
    func: Callable,
    *args: Any,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    delay: float = 0.0,
    **kwargs: Any,
) -> Any:
    """Call *func* with retry logic without using the decorator syntax.

    Args:
        func: Callable to invoke.
        *args: Positional arguments forwarded to *func*.
        exceptions: Exception types to retry on.
        max_attempts: Maximum number of attempts.
        delay: Delay in seconds between attempts.
        **kwargs: Keyword arguments forwarded to *func*.

    Returns:
        Return value of *func*.

    Raises:
        MaxRetriesExceeded: after all attempts are exhausted.
    """
    decorated = retry(exceptions=exceptions, max_attempts=max_attempts, delay=delay)(func)
    return decorated(*args, **kwargs)
