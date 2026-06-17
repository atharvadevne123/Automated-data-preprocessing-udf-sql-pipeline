"""Tests for pipeline/deduplicator.py."""

from __future__ import annotations

import pytest

from pipeline.deduplicator import DeduplicationStats, RecordDeduplicator, _record_hash


class TestRecordHash:
    def test_same_record_same_hash(self) -> None:
        r = {"a": 1, "b": "x"}
        assert _record_hash(r, None) == _record_hash(r, None)

    def test_different_records_different_hash(self) -> None:
        assert _record_hash({"a": 1}, None) != _record_hash({"a": 2}, None)

    def test_key_fields_subset(self) -> None:
        r1 = {"id": "abc", "noise": 1}
        r2 = {"id": "abc", "noise": 2}
        assert _record_hash(r1, ["id"]) == _record_hash(r2, ["id"])

    def test_key_fields_different(self) -> None:
        r1 = {"id": "abc"}
        r2 = {"id": "xyz"}
        assert _record_hash(r1, ["id"]) != _record_hash(r2, ["id"])


class TestDeduplicationStats:
    def test_initial_state(self) -> None:
        s = DeduplicationStats()
        assert s.total_seen == 0
        assert s.duplicates_dropped == 0
        assert s.unique_count == 0

    def test_unique_count(self) -> None:
        s = DeduplicationStats()
        s.total_seen = 10
        s.duplicates_dropped = 3
        assert s.unique_count == 7

    def test_to_dict(self) -> None:
        s = DeduplicationStats()
        s.total_seen = 5
        s.duplicates_dropped = 2
        d = s.to_dict()
        assert d["total_seen"] == 5
        assert d["duplicates_dropped"] == 2
        assert d["unique_count"] == 3

    def test_reset(self) -> None:
        s = DeduplicationStats()
        s.total_seen = 100
        s.duplicates_dropped = 50
        s.reset()
        assert s.total_seen == 0
        assert s.duplicates_dropped == 0


class TestRecordDeduplicator:
    def test_unique_records_pass_through(self) -> None:
        dedup = RecordDeduplicator()
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        result = list(dedup.deduplicate(iter(records)))
        assert len(result) == 3

    def test_duplicates_dropped(self) -> None:
        dedup = RecordDeduplicator()
        records = [{"id": 1}, {"id": 1}, {"id": 2}]
        result = list(dedup.deduplicate(iter(records)))
        assert len(result) == 2

    def test_key_fields(self) -> None:
        dedup = RecordDeduplicator(key_fields=["id"])
        records = [{"id": "a", "val": 1}, {"id": "a", "val": 2}, {"id": "b", "val": 3}]
        result = list(dedup.deduplicate(iter(records)))
        assert len(result) == 2

    def test_track_stats(self) -> None:
        dedup = RecordDeduplicator(track_stats=True)
        records = [{"x": 1}, {"x": 1}, {"x": 2}]
        list(dedup.deduplicate(iter(records)))
        assert dedup.stats.total_seen == 3
        assert dedup.stats.duplicates_dropped == 1
        assert dedup.stats.unique_count == 2

    def test_is_duplicate_registers(self) -> None:
        dedup = RecordDeduplicator()
        r = {"k": "v"}
        assert not dedup.is_duplicate(r)
        assert dedup.is_duplicate(r)

    def test_seen_count(self) -> None:
        dedup = RecordDeduplicator()
        list(dedup.deduplicate(iter([{"a": 1}, {"b": 2}, {"a": 1}])))
        assert dedup.seen_count == 2

    def test_reset_clears_seen_set(self) -> None:
        dedup = RecordDeduplicator()
        list(dedup.deduplicate(iter([{"x": 1}])))
        dedup.reset()
        assert dedup.seen_count == 0
        assert not dedup.is_duplicate({"x": 1})

    def test_empty_input(self) -> None:
        dedup = RecordDeduplicator()
        result = list(dedup.deduplicate(iter([])))
        assert result == []

    @pytest.mark.parametrize("n_dupes", [1, 5, 100])
    def test_all_duplicates(self, n_dupes: int) -> None:
        dedup = RecordDeduplicator()
        records = [{"id": "same"}] * n_dupes
        result = list(dedup.deduplicate(iter(records)))
        assert len(result) == 1
