"""Tests for pipeline/joiner.py RecordJoiner."""

from __future__ import annotations

import pytest

from pipeline.joiner import RecordJoiner

BUSINESSES = [
    {"business_id": "biz1", "name": "Café A", "stars": 4.5},
    {"business_id": "biz2", "name": "Diner B", "stars": 3.0},
]

REVIEWS = [
    {"review_id": "r1", "business_id": "biz1", "stars": 5},
    {"review_id": "r2", "business_id": "biz2", "stars": 2},
    {"review_id": "r3", "business_id": "biz_unknown", "stars": 4},
]


class TestRecordJoiner:
    def test_load_lookup_count(self):
        joiner = RecordJoiner(lookup_key="business_id")
        count = joiner.load_lookup(BUSINESSES)
        assert count == 2

    def test_lookup_size_property(self):
        joiner = RecordJoiner(lookup_key="business_id")
        joiner.load_lookup(BUSINESSES)
        assert joiner.lookup_size == 2

    def test_join_enriches_matching(self):
        joiner = RecordJoiner(lookup_key="business_id")
        joiner.load_lookup(BUSINESSES)
        result = joiner.join_record({"review_id": "r1", "business_id": "biz1"})
        assert result["name"] == "Café A"

    def test_join_no_match_preserved(self):
        joiner = RecordJoiner(lookup_key="business_id")
        joiner.load_lookup(BUSINESSES)
        result = joiner.join_record({"review_id": "r3", "business_id": "biz_unknown"})
        assert result["review_id"] == "r3"
        assert "name" not in result

    def test_prefix_avoids_collision(self):
        joiner = RecordJoiner(lookup_key="business_id", prefix="biz_")
        joiner.load_lookup(BUSINESSES)
        result = joiner.join_record({"business_id": "biz1", "stars": 5})
        assert "biz_name" in result
        assert result["stars"] == 5

    def test_full_join(self):
        joiner = RecordJoiner(lookup_key="business_id")
        joiner.load_lookup(BUSINESSES)
        results = joiner.join(REVIEWS)
        assert len(results) == 3
        matched = [r for r in results if "name" in r]
        assert len(matched) == 2

    def test_join_unmatched(self):
        joiner = RecordJoiner(lookup_key="business_id")
        joiner.load_lookup(BUSINESSES)
        unmatched = joiner.join_unmatched(REVIEWS)
        assert len(unmatched) == 1
        assert unmatched[0]["business_id"] == "biz_unknown"

    def test_empty_lookup(self):
        joiner = RecordJoiner(lookup_key="business_id")
        joiner.load_lookup([])
        result = joiner.join_record({"business_id": "biz1"})
        assert "name" not in result

    def test_reload_lookup_replaces_old(self):
        joiner = RecordJoiner(lookup_key="business_id")
        joiner.load_lookup(BUSINESSES)
        joiner.load_lookup([{"business_id": "new_biz", "name": "New"}])
        assert joiner.lookup_size == 1

    @pytest.mark.parametrize(
        "biz_id,expected_name",
        [
            ("biz1", "Café A"),
            ("biz2", "Diner B"),
        ],
    )
    def test_parametrized_join(self, biz_id, expected_name):
        joiner = RecordJoiner(lookup_key="business_id")
        joiner.load_lookup(BUSINESSES)
        result = joiner.join_record({"business_id": biz_id})
        assert result["name"] == expected_name
