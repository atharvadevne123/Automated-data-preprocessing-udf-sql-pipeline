"""Pipeline metrics helpers for tracking processing statistics."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MemoryMetrics:
    """Tracks memory usage at a point in time using the tracemalloc stdlib module.

    Attributes:
        peak_bytes: Peak memory in bytes since last reset.
        current_bytes: Current memory usage in bytes.
        snapshot_label: Optional label for this measurement.

    Example::

        MemoryMetrics.start_tracing()
        do_work()
        m = MemoryMetrics.snapshot("after_load")
        print(m.peak_mb)
    """

    peak_bytes: int = 0
    current_bytes: int = 0
    snapshot_label: str = ""

    @staticmethod
    def start_tracing() -> None:
        """Start or reset tracemalloc tracing."""
        import tracemalloc

        if not tracemalloc.is_tracing():
            tracemalloc.start()
        else:
            tracemalloc.reset_peak()

    @classmethod
    def snapshot(cls, label: str = "") -> "MemoryMetrics":
        """Capture current and peak memory usage.

        Args:
            label: Optional human-readable label for this snapshot.

        Returns:
            MemoryMetrics with current and peak byte counts.
        """
        import tracemalloc

        if not tracemalloc.is_tracing():
            tracemalloc.start()
        current, peak = tracemalloc.get_traced_memory()
        return cls(peak_bytes=peak, current_bytes=current, snapshot_label=label)

    @property
    def peak_mb(self) -> float:
        """Peak memory in megabytes."""
        return self.peak_bytes / (1024 * 1024)

    @property
    def current_mb(self) -> float:
        """Current memory in megabytes."""
        return self.current_bytes / (1024 * 1024)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict."""
        return {
            "label": self.snapshot_label,
            "current_bytes": self.current_bytes,
            "current_mb": round(self.current_mb, 4),
            "peak_bytes": self.peak_bytes,
            "peak_mb": round(self.peak_mb, 4),
        }


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


@dataclass
class ValidationMetrics:
    """Tracks outcomes of a batch validation pass.

    Attributes:
        total: Total records examined.
        valid: Records that passed all validations.
        invalid: Records that failed at least one validation.
        skipped: Records skipped (e.g. empty lines, non-JSON).
    """

    total: int = 0
    valid: int = 0
    invalid: int = 0
    skipped: int = 0

    @property
    def error_rate(self) -> float:
        """Fraction of examined records that were invalid."""
        return self.invalid / self.total if self.total > 0 else 0.0

    @property
    def valid_rate(self) -> float:
        """Fraction of examined records that were valid."""
        return self.valid / self.total if self.total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict."""
        return {
            "total": self.total,
            "valid": self.valid,
            "invalid": self.invalid,
            "skipped": self.skipped,
            "error_rate": round(self.error_rate, 4),
            "valid_rate": round(self.valid_rate, 4),
        }


@dataclass
class PipelineRunMetrics:
    """End-to-end metrics for a single pipeline run.

    Attributes:
        pipeline_name: Human-readable name for this run.
        input_records: Records read from source.
        output_records: Records written to sink.
        elapsed_sec: Wall-clock seconds for the run.
        validation: Nested ValidationMetrics instance.
        split_metrics: Optional SplitMetrics for file-split operations.
    """

    pipeline_name: str
    input_records: int = 0
    output_records: int = 0
    elapsed_sec: float = 0.0
    validation: ValidationMetrics = field(default_factory=ValidationMetrics)
    split_metrics: SplitMetrics | None = None

    @property
    def throughput(self) -> float:
        """Records processed per second."""
        return self.output_records / self.elapsed_sec if self.elapsed_sec > 0 else 0.0

    @property
    def drop_rate(self) -> float:
        """Fraction of input records that were not written to output."""
        if self.input_records == 0:
            return 0.0
        return max(0.0, (self.input_records - self.output_records) / self.input_records)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict of all run metrics."""
        return {
            "pipeline_name": self.pipeline_name,
            "input_records": self.input_records,
            "output_records": self.output_records,
            "elapsed_sec": round(self.elapsed_sec, 4),
            "throughput_records_per_sec": round(self.throughput, 1),
            "drop_rate": round(self.drop_rate, 4),
            "validation": self.validation.to_dict(),
        }
