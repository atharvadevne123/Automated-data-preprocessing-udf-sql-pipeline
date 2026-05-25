"""Tests for validate_text_length() in utils/validators.py."""

from __future__ import annotations

import pytest

from utils.validators import ValidationError, validate_text_length


class TestValidateTextLength:
    def test_valid_text_returned_unchanged(self):
        assert validate_text_length("hello") == "hello"

    def test_empty_string_fails_default_min(self):
        with pytest.raises(ValidationError, match="too short"):
            validate_text_length("")

    def test_empty_string_ok_with_zero_min(self):
        assert validate_text_length("", min_length=0) == ""

    def test_exact_min_length_ok(self):
        assert validate_text_length("ab", min_length=2) == "ab"

    def test_below_min_length_raises(self):
        with pytest.raises(ValidationError, match="too short"):
            validate_text_length("x", min_length=5)

    def test_exact_max_length_ok(self):
        text = "a" * 100
        assert validate_text_length(text, max_length=100) == text

    def test_above_max_length_raises(self):
        with pytest.raises(ValidationError, match="too long"):
            validate_text_length("a" * 101, max_length=100)

    def test_non_string_raises(self):
        with pytest.raises(ValidationError):
            validate_text_length(123)  # type: ignore[arg-type]

    def test_none_raises(self):
        with pytest.raises(ValidationError):
            validate_text_length(None)  # type: ignore[arg-type]

    @pytest.mark.parametrize("text,min_l,max_l", [
        ("hello world", 1, 100),
        ("x", 1, 1),
        ("ab", 2, 5),
    ])
    def test_parametrized_valid(self, text, min_l, max_l):
        assert validate_text_length(text, min_length=min_l, max_length=max_l) == text

    @pytest.mark.parametrize("text,min_l,max_l", [
        ("", 1, 100),
        ("abc", 5, 10),
        ("a" * 200, 1, 100),
    ])
    def test_parametrized_invalid(self, text, min_l, max_l):
        with pytest.raises(ValidationError):
            validate_text_length(text, min_length=min_l, max_length=max_l)
