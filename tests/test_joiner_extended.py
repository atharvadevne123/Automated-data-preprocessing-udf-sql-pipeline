"""Extended tests for inner_join() and right_join() in RecordJoiner."""

from __future__ import annotations

import pytest

from pipeline.joiner import RecordJoiner


@pytest.fixture()
def joiner() -> RecordJoiner:
    j = RecordJoiner(lookup_key="business_id", prefix="biz_")
    j.load_lookup(
        [
            {"business_id": "A", "name": "Alpha"},
            {"business_id": "B", "name": "Beta"},
        ]
    )
    return j


class TestInnerJoin:
    def test_matched_only(self, joiner: RecordJoiner) -> None:
        records = [
            {"business_id": "A", "stars": 5},
            {"business_id": "Z", "stars": 3},
        ]
        result = joiner.inner_join(records)
        assert len(result) == 1
        assert result[0]["business_id"] == "A"

    def test_no_matches_empty(self, joiner: RecordJoiner) -> None:
        result = joiner.inner_join([{"business_id": "X"}])
        assert result == []

    def test_all_match(self, joiner: RecordJoiner) -> None:
        records = [{"business_id": "A"}, {"business_id": "B"}]
        result = joiner.inner_join(records)
        assert len(result) == 2

    def test_enriched_with_lookup_fields(self, joiner: RecordJoiner) -> None:
        result = joiner.inner_join([{"business_id": "A", "stars": 4}])
        assert "biz_name" in result[0]
        assert result[0]["biz_name"] == "Alpha"

    def test_empty_records(self, joiner: RecordJoiner) -> None:
        assert joiner.inner_join([]) == []


class TestRightJoin:
    def test_returns_all_lookup_entries(self, joiner: RecordJoiner) -> None:
        result = joiner.right_join([])
        assert len(result) == 2

    def test_enriches_when_match(self, joiner: RecordJoiner) -> None:
        records = [{"business_id": "A", "stars": 5}]
        result = joiner.right_join(records)
        biz_a = next(r for r in result if r.get("business_id") == "A")
        assert "stars" in biz_a

    def test_no_match_lookup_entry_still_returned(self, joiner: RecordJoiner) -> None:
        result = joiner.right_join([])
        ids = {r["business_id"] for r in result}
        assert {"A", "B"} == ids

    @pytest.mark.parametrize("fill_value", [None, "N/A", 0])
    def test_fill_value_param(self, fill_value: object, joiner: RecordJoiner) -> None:
        result = joiner.right_join([], fill_value=fill_value)
        assert len(result) == 2


class TestExistingJoin:
    def test_left_join_includes_unmatched(self, joiner: RecordJoiner) -> None:
        records = [{"business_id": "X", "stars": 3}]
        result = joiner.join(records)
        assert len(result) == 1
        assert result[0]["business_id"] == "X"

    def test_join_unmatched(self, joiner: RecordJoiner) -> None:
        records = [{"business_id": "A"}, {"business_id": "Z"}]
        unmatched = joiner.join_unmatched(records)
        assert len(unmatched) == 1
        assert unmatched[0]["business_id"] == "Z"

    def test_lookup_size(self, joiner: RecordJoiner) -> None:
        assert joiner.lookup_size == 2
