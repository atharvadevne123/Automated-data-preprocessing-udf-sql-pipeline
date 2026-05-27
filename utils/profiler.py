"""Lightweight profiling helpers for timing and memory measurement."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Generator

logger = logging.getLogger(__name__)


class FunctionProfiler:
    """Wraps a callable to record per-call timing and call count.

    Args:
        func: The callable to profile.

    Example::

        profiler = FunctionProfiler(expensive_fn)
        result = profiler("arg1")
        print(profiler.stats)
    """

    def __init__(self, func: Any) -> None:
        import functools

        self._func = func
        functools.update_wrapper(self, func)
        self._calls: int = 0
        self._total_sec: float = 0.0
        self._min_sec: float = float("inf")
        self._max_sec: float = 0.0

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            return self._func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            self._calls += 1
            self._total_sec += elapsed
            if elapsed < self._min_sec:
                self._min_sec = elapsed
            if elapsed > self._max_sec:
                self._max_sec = elapsed

    @property
    def stats(self) -> dict[str, Any]:
        """Return a dict summary of all profiling data."""
        avg = self._total_sec / self._calls if self._calls > 0 else 0.0
        return {
            "calls": self._calls,
            "total_sec": round(self._total_sec, 6),
            "avg_sec": round(avg, 6),
            "min_sec": round(self._min_sec, 6) if self._calls > 0 else 0.0,
            "max_sec": round(self._max_sec, 6),
        }

    def reset(self) -> None:
        """Reset all accumulated profiling data."""
        self._calls = 0
        self._total_sec = 0.0
        self._min_sec = float("inf")
        self._max_sec = 0.0


@contextmanager
def timed(label: str = "", log: bool = True) -> Generator[dict[str, float], None, None]:
    """Context manager that measures elapsed wall-clock time.

    Args:
        label: Human-readable label for the timing block.
        log: If True, emit an INFO log line on exit.

    Yields:
        A dict that will contain ``{"elapsed_sec": <value>}`` on exit.

    Example::

        with timed("load phase") as t:
            load_data()
        print(t["elapsed_sec"])
    """
    result: dict[str, float] = {}
    start = time.perf_counter()
    try:
        yield result
    finally:
        elapsed = time.perf_counter() - start
        result["elapsed_sec"] = elapsed
        if log:
            suffix = f" [{label}]" if label else ""
            logger.info("Elapsed%s: %.4f s", suffix, elapsed)


def profile_memory(label: str = "") -> dict[str, Any]:
    """Capture current and peak memory usage via tracemalloc.

    Starts tracing if not already running.

    Args:
        label: Optional label for this snapshot.

    Returns:
        Dict with ``current_mb``, ``peak_mb``, and ``label`` keys.
    """
    import tracemalloc

    if not tracemalloc.is_tracing():
        tracemalloc.start()
    current, peak = tracemalloc.get_traced_memory()
    return {
        "label": label,
        "current_mb": round(current / (1024 * 1024), 4),
        "peak_mb": round(peak / (1024 * 1024), 4),
    }
