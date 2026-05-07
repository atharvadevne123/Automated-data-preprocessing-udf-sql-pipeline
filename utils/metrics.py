"""Pipeline metrics helpers for tracking processing statistics."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class SplitMetrics:
    """Tracks statistics for a file-split operation.

    Attributes:
        input_file: Path of the input file as a string.
        total_lines: Total lines processed.
        num_chunks: Number of output chunks created.
        elapsed_sec: Wall-clock time in seconds.
        bytes_read: Bytes read from the input file.
    """

    input_file: str
    total_lines: int = 0
    num_chunks: int = 0
    elapsed_sec: float = 0.0
    bytes_read: int = 0
    chunk_sizes: list[int] = field(default_factory=list)

    @property
    def lines_per_second(self) -> float:
        """Processing rate in lines per second."""
        return self.total_lines / self.elapsed_sec if self.elapsed_sec > 0 else 0.0

    @property
    def mb_per_second(self) -> float:
        """Throughput in MB per second."""
        mb = self.bytes_read / (1024 * 1024)
        return mb / self.elapsed_sec if self.elapsed_sec > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict of all metrics."""
        return {
            "input_file": self.input_file,
            "total_lines": self.total_lines,
            "num_chunks": self.num_chunks,
            "elapsed_sec": round(self.elapsed_sec, 4),
            "bytes_read": self.bytes_read,
            "lines_per_second": round(self.lines_per_second, 1),
            "mb_per_second": round(self.mb_per_second, 4),
            "chunk_sizes": self.chunk_sizes,
        }


class Timer:
    """Simple wall-clock timer as a context manager.

    Usage::

        with Timer() as t:
            do_work()
        print(f"Took {t.elapsed:.3f}s")
    """

    def __init__(self) -> None:
        self._start: float = 0.0
        self.elapsed: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_: Any) -> None:
        self.elapsed = time.perf_counter() - self._start
