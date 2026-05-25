"""Tests for pipeline/transformer.py."""

from __future__ import annotations

import pytest

from pipeline.transformer import ComputedFieldAdder, FieldRenamer, TypeCoercer


class TestFieldRenamer:
    def test_basic_rename(self):
        renamer = FieldRenamer({"review_id": "id"})
        result = renamer.transform({"review_id": "abc", "stars": 5})
        assert result == {"id": "abc", "stars": 5}

    def test_passthrough_unmapped(self):
        renamer = FieldRenamer({"a": "b"})
        result = renamer.transform({"a": 1, "c": 2})
        assert result == {"b": 1, "c": 2}

    def test_drop_missing_mode(self):
        renamer = FieldRenamer({"a": "x"}, drop_missing=True)
        result = renamer.transform({"a": 1, "b": 2})
        assert result == {"x": 1}
        assert "b" not in result

    def test_empty_record(self):
        renamer = FieldRenamer({"a": "b"})
        assert renamer.transform({}) == {}

    def test_batch(self):
        renamer = FieldRenamer({"k": "v"})
        records = [{"k": i} for i in range(5)]
        result = renamer.transform_batch(records)
        assert all("v" in r for r in result)
        assert all("k" not in r for r in result)

    @pytest.mark.parametrize("key,new_key", [("a", "x"), ("b", "y"), ("c", "z")])
    def test_parametrized_rename(self, key, new_key):
        renamer = FieldRenamer({key: new_key})
        result = renamer.transform({key: 42})
        assert result[new_key] == 42


class TestTypeCoercer:
    def test_coerce_str_to_float(self):
        coercer = TypeCoercer({"stars": float})
        result = coercer.transform({"stars": "4"})
        assert result["stars"] == 4.0
        assert isinstance(result["stars"], float)

    def test_coerce_str_to_int(self):
        coercer = TypeCoercer({"count": int})
        result = coercer.transform({"count": "100"})
        assert result["count"] == 100

    def test_skip_errors_default(self):
        coercer = TypeCoercer({"val": int})
        result = coercer.transform({"val": "not_a_number"})
        assert result["val"] == "not_a_number"

    def test_raise_on_error(self):
        coercer = TypeCoercer({"val": int}, skip_errors=False)
        with pytest.raises((ValueError, TypeError)):
            coercer.transform({"val": "bad"})

    def test_missing_field_ignored(self):
        coercer = TypeCoercer({"stars": float})
        result = coercer.transform({"text": "hello"})
        assert result == {"text": "hello"}

    def test_batch_coerce(self):
        coercer = TypeCoercer({"n": int})
        records = [{"n": str(i)} for i in range(5)]
        result = coercer.transform_batch(records)
        assert [r["n"] for r in result] == [0, 1, 2, 3, 4]

    @pytest.mark.parametrize("raw,expected", [("1", 1), ("2", 2), ("3", 3)])
    def test_parametrized_coerce(self, raw, expected):
        coercer = TypeCoercer({"n": int})
        assert coercer.transform({"n": raw})["n"] == expected


class TestComputedFieldAdder:
    def test_add_total_votes(self):
        adder = ComputedFieldAdder({"total": lambda r: r.get("a", 0) + r.get("b", 0)})
        result = adder.transform({"a": 3, "b": 2})
        assert result["total"] == 5
        assert result["a"] == 3

    def test_error_is_skipped(self):
        adder = ComputedFieldAdder({"x": lambda r: 1 / 0})
        result = adder.transform({"y": 1})
        assert "x" not in result

    def test_batch(self):
        adder = ComputedFieldAdder({"doubled": lambda r: r["n"] * 2})
        records = [{"n": i} for i in range(4)]
        result = adder.transform_batch(records)
        assert [r["doubled"] for r in result] == [0, 2, 4, 6]

    def test_setdefault_not_overwritten(self):
        adder = ComputedFieldAdder({"x": lambda r: 99})
        result = adder.transform({"x": 10})
        assert result["x"] == 99
