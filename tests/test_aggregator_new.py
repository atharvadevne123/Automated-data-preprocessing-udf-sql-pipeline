"""Tests for new StatsAggregator methods: filter_businesses, median_stars, mean_stars."""

from __future__ import annotations

import pytest

from pipeline.aggregator import StatsAggregator


def _make_agg_with_reviews():
    agg = StatsAggregator()
    reviews = [
        {"business_id": "b1", "stars": 5, "useful": 1, "sentiment_label": "positive"},
        {"business_id": "b1", "stars": 4, "useful": 0, "sentiment_label": "positive"},
        {"business_id": "b1", "stars": 4, "useful": 0, "sentiment_label": "positive"},
        {"business_id": "b1", "stars": 4, "useful": 0, "sentiment_label": "positive"},
        {"business_id": "b1", "stars": 5, "useful": 0, "sentiment_label": "positive"},
        {"business_id": "b2", "stars": 2, "useful": 0, "sentiment_label": "negative"},
        {"business_id": "b2", "stars": 1, "useful": 0, "sentiment_label": "negative"},
        {"business_id": "b2", "stars": 2, "useful": 0, "sentiment_label": "negative"},
        {"business_id": "b2", "stars": 3, "useful": 0, "sentiment_label": "neutral"},
        {"business_id": "b2", "stars": 2, "useful": 0, "sentiment_label": "negative"},
    ]
    agg.add_batch(reviews)
    return agg


class TestFilterBusinesses:
    def test_no_filters_returns_all(self):
        agg = _make_agg_with_reviews()
        result = agg.filter_businesses(min_reviews=0)
        assert len(result) == 2

    def test_min_reviews_filter(self):
        agg = _make_agg_with_reviews()
        result = agg.filter_businesses(min_reviews=10)
        assert len(result) == 0

    def test_min_stars_filter(self):
        agg = _make_agg_with_reviews()
        result = agg.filter_businesses(min_avg_stars=4.0)
        assert all(b.average_stars >= 4.0 for b in result)

    def test_max_stars_filter(self):
        agg = _make_agg_with_reviews()
        result = agg.filter_businesses(max_avg_stars=3.0)
        assert all(b.average_stars <= 3.0 for b in result)

    def test_sorted_descending(self):
        agg = _make_agg_with_reviews()
        result = agg.filter_businesses()
        stars = [b.average_stars for b in result]
        assert stars == sorted(stars, reverse=True)

    @pytest.mark.parametrize("min_r,expected_count", [(0, 2), (5, 2), (6, 0)])
    def test_parametrized_min_reviews(self, min_r, expected_count):
        agg = _make_agg_with_reviews()
        assert len(agg.filter_businesses(min_reviews=min_r)) == expected_count


class TestMedianStars:
    def test_median_with_data(self):
        agg = StatsAggregator()
        agg.add_batch([
            {"stars": 1, "sentiment_label": "negative"},
            {"stars": 3, "sentiment_label": "neutral"},
            {"stars": 5, "sentiment_label": "positive"},
        ])
        assert agg.median_stars() == 3.0

    def test_median_empty(self):
        agg = StatsAggregator()
        assert agg.median_stars() == 0.0

    def test_median_even_count(self):
        agg = StatsAggregator()
        agg.add_batch([
            {"stars": 2, "sentiment_label": "negative"},
            {"stars": 4, "sentiment_label": "positive"},
        ])
        assert agg.median_stars() == pytest.approx(3.0)

    def test_median_all_same(self):
        agg = StatsAggregator()
        for _ in range(5):
            agg.add({"stars": 3, "sentiment_label": "neutral"})
        assert agg.median_stars() == 3.0


class TestMeanStars:
    def test_mean_empty(self):
        agg = StatsAggregator()
        assert agg.mean_stars() == 0.0

    def test_mean_single(self):
        agg = StatsAggregator()
        agg.add({"stars": 4, "sentiment_label": "positive"})
        assert agg.mean_stars() == pytest.approx(4.0)

    def test_mean_mixed(self):
        agg = StatsAggregator()
        agg.add_batch([
            {"stars": 2, "sentiment_label": "negative"},
            {"stars": 4, "sentiment_label": "positive"},
        ])
        assert agg.mean_stars() == pytest.approx(3.0)
