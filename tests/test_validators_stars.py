"""Tests for new validate_stars() and validate_review_id() functions."""

from __future__ import annotations

import pytest

from utils.validators import ValidationError, validate_review_id, validate_stars


class TestValidateStars:
    def test_valid_float(self) -> None:
        assert validate_stars(3.5) == 3.5

    def test_valid_string_float(self) -> None:
        assert validate_stars("4.0") == pytest.approx(4.0)

    def test_valid_int(self) -> None:
        assert validate_stars(5) == 5.0

    def test_min_boundary(self) -> None:
        assert validate_stars(1.0) == 1.0

    def test_max_boundary(self) -> None:
        assert validate_stars(5.0) == 5.0

    def test_below_min_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"\[1.0, 5.0\]"):
            validate_stars(0.9)

    def test_above_max_raises(self) -> None:
        with pytest.raises(ValidationError):
            validate_stars(5.1)

    def test_non_numeric_raises(self) -> None:
        with pytest.raises(ValidationError):
            validate_stars("not-a-number")

    def test_none_raises(self) -> None:
        with pytest.raises(ValidationError):
            validate_stars(None)  # type: ignore[arg-type]

    @pytest.mark.parametrize("value,expected", [(1, 1.0), (2.5, 2.5), (5, 5.0), ("3", 3.0)])
    def test_parametrized_valid(self, value: object, expected: float) -> None:
        assert validate_stars(value) == pytest.approx(expected)

    @pytest.mark.parametrize("bad_value", [0, 6, -1, "abc", None, []])
    def test_parametrized_invalid(self, bad_value: object) -> None:
        with pytest.raises((ValidationError, TypeError)):
            validate_stars(bad_value)  # type: ignore[arg-type]


class TestValidateReviewId:
    def test_valid_alphanumeric(self) -> None:
        assert validate_review_id("abc123") == "abc123"

    def test_valid_with_hyphen(self) -> None:
        assert validate_review_id("review-001") == "review-001"

    def test_valid_with_underscore(self) -> None:
        assert validate_review_id("review_001") == "review_001"

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValidationError):
            validate_review_id("")

    def test_non_string_raises(self) -> None:
        with pytest.raises(ValidationError):
            validate_review_id(None)  # type: ignore[arg-type]

    def test_too_long_raises(self) -> None:
        with pytest.raises(ValidationError):
            validate_review_id("a" * 101)

    def test_invalid_characters_raises(self) -> None:
        with pytest.raises(ValidationError):
            validate_review_id("review@id!")

    def test_max_length_valid(self) -> None:
        assert validate_review_id("a" * 100) == "a" * 100

    @pytest.mark.parametrize("valid_id", ["abc", "ABC-123", "x_y_z", "a" * 22])
    def test_parametrized_valid(self, valid_id: str) -> None:
        assert validate_review_id(valid_id) == valid_id
