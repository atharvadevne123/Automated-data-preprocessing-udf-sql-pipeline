"""Tests for TimeSeries class in utils/metrics.py."""

from __future__ import annotations

import pytest

from utils.metrics import TimeSeries


class TestTimeSeries:
    def test_initial_state(self) -> None:
        ts = TimeSeries("latency")
        assert ts.count() == 0
        assert ts.mean() == 0.0
        assert ts.last() is None

    def test_record_and_count(self) -> None:
        ts = TimeSeries("test")
        ts.record(1.0)
        ts.record(2.0)
        assert ts.count() == 2

    def test_mean(self) -> None:
        ts = TimeSeries("test")
        for v in [1.0, 2.0, 3.0]:
            ts.record(v)
        assert ts.mean() == pytest.approx(2.0)

    def test_min(self) -> None:
        ts = TimeSeries("test")
        for v in [5.0, 1.0, 3.0]:
            ts.record(v)
        assert ts.min() == 1.0

    def test_max(self) -> None:
        ts = TimeSeries("test")
        for v in [5.0, 1.0, 3.0]:
            ts.record(v)
        assert ts.max() == 5.0

    def test_last(self) -> None:
        ts = TimeSeries("test")
        ts.record(10.0)
        ts.record(20.0)
        assert ts.last() == 20.0

    def test_max_points_eviction(self) -> None:
        ts = TimeSeries("test", max_points=3)
        for i in range(10):
            ts.record(float(i))
        assert ts.count() == 3

    def test_to_dict(self) -> None:
        ts = TimeSeries("my_metric")
        ts.record(5.0)
        d = ts.to_dict()
        assert d["name"] == "my_metric"
        assert d["count"] == 1
        assert d["mean"] == 5.0
        assert d["last"] == 5.0

    def test_reset(self) -> None:
        ts = TimeSeries("test")
        ts.record(1.0)
        ts.reset()
        assert ts.count() == 0
        assert ts.last() is None

    def test_empty_min(self) -> None:
        ts = TimeSeries("empty")
        assert ts.min() == 0.0

    def test_empty_max(self) -> None:
        ts = TimeSeries("empty")
        assert ts.max() == 0.0

    @pytest.mark.parametrize(
        "n_records,max_points,expected_count",
        [
            (5, 10, 5),
            (10, 5, 5),
            (1, 1, 1),
        ],
    )
    def test_max_points_parametrized(self, n_records: int, max_points: int, expected_count: int) -> None:
        ts = TimeSeries("test", max_points=max_points)
        for i in range(n_records):
            ts.record(float(i))
        assert ts.count() == expected_count
