"""Tests for ValueMapper and FieldDropper in pipeline/transformer.py."""

from __future__ import annotations

import pytest

from pipeline.transformer import FieldDropper, ValueMapper


class TestValueMapper:
    def test_maps_value(self) -> None:
        mapper = ValueMapper("status", {"active": "ACTIVE"})
        result = mapper.transform({"status": "active"})
        assert result["status"] == "ACTIVE"

    def test_unmapped_value_unchanged(self) -> None:
        mapper = ValueMapper("status", {"active": "ACTIVE"})
        result = mapper.transform({"status": "unknown"})
        assert result["status"] == "unknown"

    def test_default_applied_to_unmapped(self) -> None:
        mapper = ValueMapper("x", {"a": "A"}, default="OTHER")
        result = mapper.transform({"x": "b"})
        assert result["x"] == "OTHER"

    def test_missing_field_unchanged(self) -> None:
        mapper = ValueMapper("x", {"a": "A"})
        result = mapper.transform({"y": 1})
        assert result == {"y": 1}

    def test_does_not_mutate_original(self) -> None:
        mapper = ValueMapper("k", {"old": "new"})
        original = {"k": "old"}
        mapper.transform(original)
        assert original["k"] == "old"

    def test_transform_batch(self) -> None:
        mapper = ValueMapper("n", {1: "one", 2: "two"})
        result = mapper.transform_batch([{"n": 1}, {"n": 2}, {"n": 3}])
        assert result[0]["n"] == "one"
        assert result[1]["n"] == "two"
        assert result[2]["n"] == 3

    def test_none_default_keeps_original(self) -> None:
        mapper = ValueMapper("k", {"a": "A"}, default=None)
        result = mapper.transform({"k": "b"})
        assert result["k"] == "b"

    @pytest.mark.parametrize("val,expected", [("pos", "positive"), ("neg", "negative"), ("neu", "neutral")])
    def test_sentiment_mapping(self, val: str, expected: str) -> None:
        mapper = ValueMapper(
            "sent",
            {"pos": "positive", "neg": "negative", "neu": "neutral"},
        )
        assert mapper.transform({"sent": val})["sent"] == expected


class TestFieldDropper:
    def test_drops_listed_fields(self) -> None:
        dropper = FieldDropper(["_internal", "debug"])
        record = {"data": 1, "_internal": True, "debug": "verbose"}
        result = dropper.transform(record)
        assert "_internal" not in result
        assert "debug" not in result
        assert "data" in result

    def test_nonexistent_field_safe(self) -> None:
        dropper = FieldDropper(["missing_field"])
        result = dropper.transform({"a": 1})
        assert result == {"a": 1}

    def test_drop_all_fields(self) -> None:
        dropper = FieldDropper(["a", "b"])
        result = dropper.transform({"a": 1, "b": 2})
        assert result == {}

    def test_does_not_mutate_original(self) -> None:
        dropper = FieldDropper(["x"])
        original = {"x": 1, "y": 2}
        dropper.transform(original)
        assert "x" in original

    def test_transform_batch(self) -> None:
        dropper = FieldDropper(["_id"])
        records = [{"_id": 1, "v": "a"}, {"_id": 2, "v": "b"}]
        result = dropper.transform_batch(records)
        assert all("_id" not in r for r in result)
        assert all("v" in r for r in result)

    def test_empty_drop_list(self) -> None:
        dropper = FieldDropper([])
        record = {"a": 1, "b": 2}
        assert dropper.transform(record) == record

    @pytest.mark.parametrize("fields_to_drop", [["a"], ["a", "b"], []])
    def test_parametrized_drops(self, fields_to_drop: list[str]) -> None:
        dropper = FieldDropper(fields_to_drop)
        record = {"a": 1, "b": 2, "c": 3}
        result = dropper.transform(record)
        for f in fields_to_drop:
            assert f not in result
        remaining = set(record) - set(fields_to_drop)
        for f in remaining:
            assert f in result
