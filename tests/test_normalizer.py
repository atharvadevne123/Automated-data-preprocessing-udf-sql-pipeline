"""Tests for pipeline/normalizer.py."""

from __future__ import annotations

import pytest

from pipeline.normalizer import FieldNormalizer


class TestFieldNormalizer:
    def test_basic_registration_and_normalise(self) -> None:
        norm = FieldNormalizer()
        norm.register("name", str.upper)
        result = norm.normalise({"name": "alice"})
        assert result["name"] == "ALICE"

    def test_multiple_funcs_on_same_field(self) -> None:
        norm = FieldNormalizer()
        norm.register("text", str.strip)
        norm.register("text", str.upper)
        result = norm.normalise({"text": "  hello  "})
        assert result["text"] == "HELLO"

    def test_unregistered_fields_unchanged(self) -> None:
        norm = FieldNormalizer()
        norm.register("a", str.upper)
        result = norm.normalise({"a": "x", "b": "unchanged"})
        assert result["b"] == "unchanged"

    def test_missing_field_skipped_by_default(self) -> None:
        norm = FieldNormalizer()
        norm.register("missing", str.upper)
        result = norm.normalise({"other": "val"})
        assert result == {"other": "val"}

    def test_strict_mode_raises_on_missing_field(self) -> None:
        norm = FieldNormalizer(strict=True)
        norm.register("required_field", str.upper)
        with pytest.raises(KeyError):
            norm.normalise({"other": "val"})

    def test_normalise_batch(self) -> None:
        norm = FieldNormalizer()
        norm.register("k", str.lower)
        result = norm.normalise_batch([{"k": "A"}, {"k": "B"}])
        assert result[0]["k"] == "a"
        assert result[1]["k"] == "b"

    def test_to_float(self) -> None:
        assert FieldNormalizer.to_float("3.5") == 3.5
        assert FieldNormalizer.to_float("bad") == 0.0
        assert FieldNormalizer.to_float(None) == 0.0

    def test_to_int(self) -> None:
        assert FieldNormalizer.to_int("7") == 7
        assert FieldNormalizer.to_int("abc") == 0
        assert FieldNormalizer.to_int(3.9) == 3

    def test_strip_whitespace(self) -> None:
        assert FieldNormalizer.strip_whitespace("  hello  ") == "hello"
        assert FieldNormalizer.strip_whitespace(None) == ""

    def test_normalise_stars_clamp(self) -> None:
        assert FieldNormalizer.normalise_stars(0) == 1.0
        assert FieldNormalizer.normalise_stars(6) == 5.0
        assert FieldNormalizer.normalise_stars(3) == 3.0

    def test_normalise_stars_invalid(self) -> None:
        assert FieldNormalizer.normalise_stars("bad") == 0.0

    def test_normalise_phone(self) -> None:
        assert FieldNormalizer.normalise_phone("+1 (800) 555-0100") == "18005550100"
        assert FieldNormalizer.normalise_phone(None) == ""

    def test_lowercase_strip(self) -> None:
        assert FieldNormalizer.lowercase_strip("  HELLO  ") == "hello"
        assert FieldNormalizer.lowercase_strip(None) == ""

    def test_normalise_does_not_mutate_original(self) -> None:
        norm = FieldNormalizer()
        norm.register("val", lambda v: v * 2)
        original = {"val": 5, "other": "x"}
        norm.normalise(original)
        assert original["val"] == 5

    @pytest.mark.parametrize("val,expected", [("1.0", 1.0), ("5", 5.0), (4, 4.0)])
    def test_to_float_parametrized(self, val: object, expected: float) -> None:
        assert FieldNormalizer.to_float(val) == expected
