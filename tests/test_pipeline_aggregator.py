"""Tests for pipeline.aggregator.StatsAggregator."""

from __future__ import annotations

import pytest

from pipeline.aggregator import BusinessStats, GlobalStats, StatsAggregator


def _review(business_id="b1", stars=4.0, useful=0, sentiment_label="positive"):
    return {"business_id": business_id, "stars": stars, "useful": useful, "sentiment_label": sentiment_label}


class TestBusinessStats:
    def test_average_stars_no_reviews(self):
        b = BusinessStats(business_id="b1")
        assert b.average_stars == 0.0

    def test_average_stars_calculated(self):
        b = BusinessStats(business_id="b1", review_count=2, star_sum=8.0)
        assert b.average_stars == 4.0

    def test_to_dict_has_all_keys(self):
        b = BusinessStats(business_id="b1", review_count=5, star_sum=20.0)
        d = b.to_dict()
        assert "business_id" in d
        assert "average_stars" in d
        assert "review_count" in d


class TestGlobalStats:
    def test_to_dict_has_keys(self):
        g = GlobalStats()
        d = g.to_dict()
        assert "total_records" in d
        assert "star_distribution" in d
        assert "sentiment_counts" in d


class TestStatsAggregator:
    def test_add_single_record(self):
        agg = StatsAggregator()
        agg.add(_review())
        assert agg.global_stats().total_records == 1

    def test_add_multiple_records(self):
        agg = StatsAggregator()
        for _ in range(10):
            agg.add(_review())
        assert agg.global_stats().total_records == 10

    def test_business_stats_tracked(self):
        agg = StatsAggregator()
        agg.add(_review(business_id="b1", stars=4.0))
        agg.add(_review(business_id="b1", stars=5.0))
        bstats = agg.business_stats("b1")
        assert bstats is not None
        assert bstats.review_count == 2
        assert bstats.average_stars == 4.5

    def test_business_stats_none_for_unknown(self):
        agg = StatsAggregator()
        assert agg.business_stats("unknown") is None

    def test_positive_count_incremented(self):
        agg = StatsAggregator()
        agg.add(_review(stars=5.0))
        agg.add(_review(stars=4.0))
        agg.add(_review(stars=3.0))
        b = agg.business_stats("b1")
        assert b.positive_count == 2

    def test_negative_count_incremented(self):
        agg = StatsAggregator()
        agg.add(_review(stars=1.0))
        agg.add(_review(stars=2.0))
        b = agg.business_stats("b1")
        assert b.negative_count == 2

    def test_star_distribution(self):
        agg = StatsAggregator()
        for stars in [1.0, 2.0, 5.0, 5.0]:
            agg.add(_review(stars=stars))
        dist = agg.global_stats().star_distribution
        assert dist[5] == 2
        assert dist[1] == 1

    def test_sentiment_counts_tracked(self):
        agg = StatsAggregator()
        agg.add(_review(sentiment_label="positive"))
        agg.add(_review(sentiment_label="negative"))
        agg.add(_review(sentiment_label="positive"))
        counts = agg.global_stats().sentiment_counts
        assert counts["positive"] == 2
        assert counts["negative"] == 1

    def test_add_batch(self):
        agg = StatsAggregator()
        agg.add_batch([_review() for _ in range(5)])
        assert agg.global_stats().total_records == 5

    def test_top_businesses_sorted_by_rating(self):
        agg = StatsAggregator()
        for _ in range(5):
            agg.add(_review(business_id="high", stars=5.0))
            agg.add(_review(business_id="low", stars=1.0))
        top = agg.top_businesses(n=1)
        assert top[0].business_id == "high"

    def test_top_businesses_min_review_threshold(self):
        agg = StatsAggregator()
        for _ in range(3):
            agg.add(_review(business_id="few", stars=5.0))
        top = agg.top_businesses()
        assert all(b.business_id != "few" for b in top)

    def test_all_business_stats_returns_list(self):
        agg = StatsAggregator()
        agg.add(_review(business_id="b1"))
        agg.add(_review(business_id="b2"))
        stats = agg.all_business_stats()
        assert len(stats) == 2

    def test_reset_clears_state(self):
        agg = StatsAggregator()
        agg.add(_review())
        agg.reset()
        assert agg.global_stats().total_records == 0
        assert agg.business_stats("b1") is None

    @pytest.mark.parametrize("stars", [1.0, 2.0, 3.0, 4.0, 5.0])
    def test_each_star_rating(self, stars):
        agg = StatsAggregator()
        agg.add(_review(stars=stars))
        assert agg.global_stats().total_records == 1

    def test_record_without_business_id(self):
        agg = StatsAggregator()
        agg.add({"stars": 3.0})
        assert agg.global_stats().total_records == 1
