"""Tests for utils/schema_validator.py."""

from __future__ import annotations

import pytest

from utils.schema_validator import FieldSpec, RecordSchemaValidator, ValidationResult


class TestFieldSpec:
    def test_valid_type(self) -> None:
        spec = FieldSpec("name", type="str")
        assert spec.type == "str"

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported type"):
            FieldSpec("name", type="bytes")

    def test_default_required(self) -> None:
        spec = FieldSpec("x")
        assert spec.required is True

    def test_allowed_values_empty_by_default(self) -> None:
        spec = FieldSpec("x")
        assert spec.allowed_values == set()


class TestValidationResult:
    def test_valid(self) -> None:
        r = ValidationResult(valid=True)
        assert r.valid

    def test_to_dict(self) -> None:
        r = ValidationResult(valid=False, errors=["err"])
        d = r.to_dict()
        assert d["valid"] is False
        assert "err" in d["errors"]


class TestRecordSchemaValidator:
    def _validator(self) -> RecordSchemaValidator:
        return RecordSchemaValidator([
            FieldSpec("stars", type="float", min_value=1.0, max_value=5.0),
            FieldSpec("text", type="str", min_length=1),
            FieldSpec("category", type="str", required=False),
        ])

    def test_valid_record(self) -> None:
        v = self._validator()
        result = v.validate({"stars": 4.0, "text": "Great!"})
        assert result.valid

    def test_missing_required_field(self) -> None:
        v = self._validator()
        result = v.validate({"stars": 3.0})
        assert not result.valid
        assert any("text" in e for e in result.errors)

    def test_wrong_type(self) -> None:
        v = self._validator()
        result = v.validate({"stars": "not-float", "text": "hi"})
        assert not result.valid

    def test_min_value_violation(self) -> None:
        v = self._validator()
        result = v.validate({"stars": 0.5, "text": "x"})
        assert not result.valid

    def test_max_value_violation(self) -> None:
        v = self._validator()
        result = v.validate({"stars": 6.0, "text": "x"})
        assert not result.valid

    def test_min_length_violation(self) -> None:
        v = self._validator()
        result = v.validate({"stars": 3.0, "text": ""})
        assert not result.valid

    def test_optional_field_absent(self) -> None:
        v = self._validator()
        result = v.validate({"stars": 3.0, "text": "hello"})
        assert result.valid

    def test_allowed_values(self) -> None:
        v = RecordSchemaValidator([
            FieldSpec("status", type="str", allowed_values={"active", "inactive"}),
        ])
        assert v.validate({"status": "active"}).valid
        assert not v.validate({"status": "pending"}).valid

    def test_validate_batch(self) -> None:
        v = self._validator()
        records = [
            {"stars": 4.0, "text": "ok"},
            {"stars": 0.0, "text": "bad"},
        ]
        results = v.validate_batch(records)
        assert len(results) == 2
        assert results[0].valid
        assert not results[1].valid

    def test_filter_valid(self) -> None:
        v = self._validator()
        records = [
            {"stars": 4.0, "text": "ok"},
            {"stars": 99.0, "text": "bad"},
        ]
        valid = v.filter_valid(records)
        assert len(valid) == 1

    def test_max_length_string(self) -> None:
        v = RecordSchemaValidator([
            FieldSpec("code", type="str", max_length=3),
        ])
        assert v.validate({"code": "ab"}).valid
        assert not v.validate({"code": "abcd"}).valid

    def test_any_type_accepts_any(self) -> None:
        v = RecordSchemaValidator([FieldSpec("x", type="any")])
        assert v.validate({"x": 42}).valid
        assert v.validate({"x": "hello"}).valid
        assert v.validate({"x": [1, 2, 3]}).valid

    @pytest.mark.parametrize("value,valid", [(1, True), (10, False)])
    def test_int_max_value(self, value: int, valid: bool) -> None:
        v = RecordSchemaValidator([FieldSpec("n", type="int", max_value=5)])
        assert v.validate({"n": value}).valid == valid
