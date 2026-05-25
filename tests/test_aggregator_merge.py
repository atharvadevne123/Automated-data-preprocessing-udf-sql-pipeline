"""Tests for StatsAggregator.merge() and BusinessStats.merge()."""

from __future__ import annotations

import pytest

from pipeline.aggregator import BusinessStats, StatsAggregator


class TestBusinessStatsMerge:
    def test_merge_sums_review_count(self):
        a = BusinessStats(business_id="b1", review_count=3, star_sum=12.0)
        b = BusinessStats(business_id="b1", review_count=2, star_sum=8.0)
        a.merge(b)
        assert a.review_count == 5

    def test_merge_sums_star_sum(self):
        a = BusinessStats(business_id="b1", review_count=2, star_sum=8.0)
        b = BusinessStats(business_id="b1", review_count=1, star_sum=3.0)
        a.merge(b)
        assert a.star_sum == pytest.approx(11.0)

    def test_merge_sums_useful(self):
        a = BusinessStats(business_id="b1", useful_sum=5)
        b = BusinessStats(business_id="b1", useful_sum=3)
        a.merge(b)
        assert a.useful_sum == 8

    def test_merge_sums_positive_count(self):
        a = BusinessStats(business_id="b1", positive_count=2)
        b = BusinessStats(business_id="b1", positive_count=3)
        a.merge(b)
        assert a.positive_count == 5

    def test_merge_sums_negative_count(self):
        a = BusinessStats(business_id="b1", negative_count=1)
        b = BusinessStats(business_id="b1", negative_count=4)
        a.merge(b)
        assert a.negative_count == 5


class TestStatsAggregatorMerge:
    def _make_agg(self, records):
        agg = StatsAggregator()
        for r in records:
            agg.add(r)
        return agg

    def test_merge_total_records(self):
        a = self._make_agg([{"stars": 4}] * 3)
        b = self._make_agg([{"stars": 2}] * 2)
        a.merge(b)
        assert a.global_stats().total_records == 5

    def test_merge_star_distribution(self):
        a = self._make_agg([{"stars": 5}] * 2)
        b = self._make_agg([{"stars": 5}] * 3)
        a.merge(b)
        assert a.global_stats().star_distribution[5] == 5

    def test_merge_sentiment_counts(self):
        a = self._make_agg([{"stars": 4, "sentiment_label": "positive"}])
        b = self._make_agg([{"stars": 4, "sentiment_label": "positive"}])
        a.merge(b)
        assert a.global_stats().sentiment_counts["positive"] == 2

    def test_merge_existing_business(self):
        a = self._make_agg([{"business_id": "b1", "stars": 4}])
        b = self._make_agg([{"business_id": "b1", "stars": 2}])
        a.merge(b)
        stats = a.business_stats("b1")
        assert stats.review_count == 2

    def test_merge_new_business(self):
        a = self._make_agg([{"business_id": "b1", "stars": 4}])
        b = self._make_agg([{"business_id": "b2", "stars": 3}])
        a.merge(b)
        assert a.business_stats("b2") is not None

    def test_merge_empty_other(self):
        a = self._make_agg([{"stars": 4}])
        b = StatsAggregator()
        a.merge(b)
        assert a.global_stats().total_records == 1

    def test_merge_average_stars_correct(self):
        a = self._make_agg([{"business_id": "bx", "stars": 4}])
        b = self._make_agg([{"business_id": "bx", "stars": 2}])
        a.merge(b)
        assert a.business_stats("bx").average_stars == pytest.approx(3.0)
