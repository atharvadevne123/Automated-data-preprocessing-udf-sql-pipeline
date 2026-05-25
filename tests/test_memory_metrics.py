"""Tests for MemoryMetrics in utils/metrics.py."""

from __future__ import annotations

import pytest

from utils.metrics import MemoryMetrics


class TestMemoryMetrics:
    def test_start_tracing_does_not_error(self):
        MemoryMetrics.start_tracing()

    def test_snapshot_returns_instance(self):
        MemoryMetrics.start_tracing()
        m = MemoryMetrics.snapshot("test")
        assert isinstance(m, MemoryMetrics)

    def test_label_stored(self):
        MemoryMetrics.start_tracing()
        m = MemoryMetrics.snapshot("my_label")
        assert m.snapshot_label == "my_label"

    def test_bytes_non_negative(self):
        MemoryMetrics.start_tracing()
        m = MemoryMetrics.snapshot()
        assert m.current_bytes >= 0
        assert m.peak_bytes >= 0

    def test_mb_conversion(self):
        m = MemoryMetrics(peak_bytes=1024 * 1024, current_bytes=512 * 1024)
        assert m.peak_mb == pytest.approx(1.0)
        assert m.current_mb == pytest.approx(0.5)

    def test_to_dict_keys(self):
        m = MemoryMetrics(peak_bytes=1000, current_bytes=500, snapshot_label="x")
        d = m.to_dict()
        for key in ("label", "current_bytes", "current_mb", "peak_bytes", "peak_mb"):
            assert key in d

    def test_to_dict_values(self):
        m = MemoryMetrics(peak_bytes=2 * 1024 * 1024, current_bytes=1024 * 1024)
        d = m.to_dict()
        assert d["peak_mb"] == pytest.approx(2.0)
        assert d["current_mb"] == pytest.approx(1.0)

    def test_snapshot_auto_starts_tracing(self):
        import tracemalloc
        tracemalloc.stop()
        m = MemoryMetrics.snapshot("auto_start")
        assert m.peak_bytes >= 0

    @pytest.mark.parametrize("peak,current", [(1000, 500), (2000, 1000), (0, 0)])
    def test_parametrized_mb(self, peak, current):
        m = MemoryMetrics(peak_bytes=peak, current_bytes=current)
        assert m.peak_mb >= 0
        assert m.current_mb >= 0
