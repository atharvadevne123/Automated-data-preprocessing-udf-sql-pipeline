"""Extended parametrized tests for StatsAggregator."""

from __future__ import annotations

import pytest

from pipeline.aggregator import StatsAggregator


def _review(business_id="b1", stars=3.0, useful=0, sentiment="neutral"):
    return {"business_id": business_id, "stars": stars, "useful": useful, "sentiment_label": sentiment}


class TestAggregatorStarDistribution:
    @pytest.mark.parametrize("star_values,expected_dist", [
        ([1.0, 2.0, 3.0, 4.0, 5.0], {1: 1, 2: 1, 3: 1, 4: 1, 5: 1}),
        ([5.0, 5.0, 5.0], {5: 3}),
        ([1.0, 1.0, 1.0], {1: 3}),
        ([3.7, 3.2, 3.5], {4: 2, 3: 1}),
    ])
    def test_star_distribution(self, star_values, expected_dist):
        agg = StatsAggregator()
        for s in star_values:
            agg.add({"stars": s, "business_id": "b"})
        dist = agg.global_stats().star_distribution
        for k, v in expected_dist.items():
            assert dist.get(k, 0) == v


class TestAggregatorSentimentCounts:
    @pytest.mark.parametrize("labels,expected", [
        (["positive"] * 5, {"positive": 5}),
        (["negative"] * 3 + ["positive"] * 2, {"negative": 3, "positive": 2}),
        (["neutral"] * 4, {"neutral": 4}),
    ])
    def test_sentiment_count(self, labels, expected):
        agg = StatsAggregator()
        for lbl in labels:
            agg.add({"stars": 3.0, "sentiment_label": lbl, "business_id": "b"})
        counts = agg.global_stats().sentiment_counts
        for k, v in expected.items():
            assert counts.get(k, 0) == v


class TestAggregatorTopBusinesses:
    def test_top_businesses_min_5_reviews(self):
        agg = StatsAggregator()
        for _ in range(4):
            agg.add(_review(business_id="few", stars=5.0))
        for _ in range(6):
            agg.add(_review(business_id="enough", stars=4.0))
        top = agg.top_businesses()
        bids = [b.business_id for b in top]
        assert "few" not in bids
        assert "enough" in bids

    def test_top_n_zero_returns_empty(self):
        agg = StatsAggregator()
        for _ in range(10):
            agg.add(_review())
        assert agg.top_businesses(n=0) == []

    def test_all_business_stats_count(self):
        agg = StatsAggregator()
        for i in range(5):
            agg.add(_review(business_id=f"b{i}"))
        assert len(agg.all_business_stats()) == 5

    @pytest.mark.parametrize("n_businesses", [1, 3, 10])
    def test_top_n_limit(self, n_businesses):
        agg = StatsAggregator()
        for bid_idx in range(n_businesses * 2):
            for _ in range(6):
                agg.add(_review(business_id=f"b{bid_idx}", stars=float(bid_idx % 5 + 1)))
        top = agg.top_businesses(n=n_businesses)
        assert len(top) <= n_businesses


class TestAggregatorBusinessStats:
    def test_positive_count_accurate(self):
        agg = StatsAggregator()
        for s in [4.0, 5.0, 3.0, 2.0, 1.0]:
            agg.add(_review(stars=s))
        b = agg.business_stats("b1")
        assert b.positive_count == 2

    def test_negative_count_accurate(self):
        agg = StatsAggregator()
        for s in [1.0, 2.0, 3.0, 4.0, 5.0]:
            agg.add(_review(stars=s))
        b = agg.business_stats("b1")
        assert b.negative_count == 2

    def test_useful_sum_accumulated(self):
        agg = StatsAggregator()
        for u in [1, 2, 3, 4]:
            agg.add(_review(useful=u))
        b = agg.business_stats("b1")
        assert b.useful_sum == 10

    def test_average_stars_precision(self):
        agg = StatsAggregator()
        for s in [1.0, 5.0]:
            agg.add(_review(stars=s))
        b = agg.business_stats("b1")
        assert b.average_stars == pytest.approx(3.0)
