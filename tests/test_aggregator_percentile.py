"""Tests for new StatsAggregator methods: percentile_stars and stddev_stars."""

from __future__ import annotations

import math

import pytest

from pipeline.aggregator import StatsAggregator


def _make_aggregator(star_list: list[float]) -> StatsAggregator:
    agg = StatsAggregator()
    for s in star_list:
        agg.add({"stars": s, "business_id": "b1", "useful": 0})
    return agg


class TestPercentileStars:
    def test_median_odd(self) -> None:
        agg = _make_aggregator([1.0, 2.0, 3.0, 4.0, 5.0])
        assert agg.percentile_stars(50) == pytest.approx(3.0)

    def test_minimum_p0(self) -> None:
        agg = _make_aggregator([2.0, 3.0, 4.0])
        assert agg.percentile_stars(0) == pytest.approx(2.0)

    def test_maximum_p100(self) -> None:
        agg = _make_aggregator([2.0, 3.0, 5.0])
        assert agg.percentile_stars(100) == pytest.approx(5.0)

    def test_empty_returns_zero(self) -> None:
        agg = StatsAggregator()
        assert agg.percentile_stars(50) == 0.0

    def test_invalid_percentile_raises(self) -> None:
        agg = _make_aggregator([3.0])
        with pytest.raises(ValueError):
            agg.percentile_stars(-1)
        with pytest.raises(ValueError):
            agg.percentile_stars(101)

    def test_p25(self) -> None:
        agg = _make_aggregator([1.0, 2.0, 3.0, 4.0])
        result = agg.percentile_stars(25)
        assert 1.0 <= result <= 2.0

    def test_p75(self) -> None:
        agg = _make_aggregator([1.0, 2.0, 3.0, 4.0])
        result = agg.percentile_stars(75)
        assert 3.0 <= result <= 4.0

    @pytest.mark.parametrize("p", [0, 25, 50, 75, 100])
    def test_result_in_valid_range(self, p: float) -> None:
        agg = _make_aggregator([1.0, 2.0, 3.0, 4.0, 5.0])
        result = agg.percentile_stars(p)
        assert 1.0 <= result <= 5.0


class TestStddevStars:
    def test_all_same_is_zero(self) -> None:
        agg = _make_aggregator([3.0, 3.0, 3.0])
        assert agg.stddev_stars() == pytest.approx(0.0)

    def test_known_stddev(self) -> None:
        agg = _make_aggregator([1.0, 3.0, 5.0])
        expected = math.sqrt(((1 - 3) ** 2 + (3 - 3) ** 2 + (5 - 3) ** 2) / 3)
        assert agg.stddev_stars() == pytest.approx(expected, rel=1e-5)

    def test_empty_returns_zero(self) -> None:
        agg = StatsAggregator()
        assert agg.stddev_stars() == 0.0

    def test_single_record_returns_zero(self) -> None:
        agg = _make_aggregator([4.0])
        assert agg.stddev_stars() == 0.0

    def test_nonnegative(self) -> None:
        agg = _make_aggregator([1.0, 2.0, 5.0])
        assert agg.stddev_stars() >= 0.0
