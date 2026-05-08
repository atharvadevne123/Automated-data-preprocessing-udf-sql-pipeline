"""Edge-case and parametrized tests for utils.validators additions."""

from __future__ import annotations

import pytest

from utils.validators import (
    sanitize_text,
    validate_email,
    validate_star_rating,
    validate_yelp_review_record,
)


class TestValidateStarRating:
    @pytest.mark.parametrize("val,expected", [
        (1, 1.0),
        (5, 5.0),
        (3.5, 3.5),
        ("4", 4.0),
        ("2.5", 2.5),
    ])
    def test_valid_values(self, val, expected):
        assert validate_star_rating(val) == expected

    @pytest.mark.parametrize("val", [0, 0.9, 5.1, 6, -1, "abc", None])
    def test_invalid_values_raise(self, val):
        with pytest.raises((ValueError, TypeError)):
            validate_star_rating(val)

    @pytest.mark.parametrize("boundary,expected", [
        (1.0, 1.0),
        (5.0, 5.0),
    ])
    def test_boundary_values_accepted(self, boundary, expected):
        assert validate_star_rating(boundary) == expected


class TestValidateEmail:
    @pytest.mark.parametrize("email", [
        "user@example.com",
        "a.b+c@sub.domain.org",
        "test123@test.co.uk",
    ])
    def test_valid_emails(self, email):
        assert validate_email(email) == email

    @pytest.mark.parametrize("bad_email", [
        "not-an-email",
        "@nodomain.com",
        "missing@",
        "",
        "spaces in@email.com",
    ])
    def test_invalid_emails_raise(self, bad_email):
        with pytest.raises(ValueError):
            validate_email(bad_email)

    def test_returns_stripped_email(self):
        result = validate_email("  user@example.com  ")
        assert result == "user@example.com"


class TestSanitizeText:
    def test_strips_leading_trailing_whitespace(self):
        assert sanitize_text("  hello  ") == "hello"

    def test_removes_null_bytes(self):
        result = sanitize_text("hello\x00world")
        assert "\x00" not in result

    def test_truncates_to_max_length(self):
        result = sanitize_text("a" * 20000, max_length=100)
        assert len(result) == 100

    def test_short_text_not_truncated(self):
        text = "short text"
        assert sanitize_text(text) == text

    @pytest.mark.parametrize("max_len", [10, 50, 100, 1000])
    def test_truncation_various_limits(self, max_len):
        text = "x" * (max_len * 2)
        result = sanitize_text(text, max_length=max_len)
        assert len(result) <= max_len

    def test_empty_string_returns_empty(self):
        assert sanitize_text("") == ""

    def test_preserves_normal_text(self):
        text = "Normal review text with punctuation! Great place."
        result = sanitize_text(text)
        assert result == text


class TestValidateYelpReviewRecord:
    def _valid_record(self):
        return {
            "review_id": "abc123",
            "user_id": "user1",
            "business_id": "biz1",
            "stars": 4,
        }

    def test_valid_record_passes(self):
        record = self._valid_record()
        result = validate_yelp_review_record(record)
        assert result["review_id"] == "abc123"

    @pytest.mark.parametrize("missing_field", ["review_id", "user_id", "business_id", "stars"])
    def test_missing_required_field_raises(self, missing_field):
        record = self._valid_record()
        del record[missing_field]
        with pytest.raises((KeyError, ValueError)):
            validate_yelp_review_record(record)

    def test_invalid_stars_raises(self):
        record = self._valid_record()
        record["stars"] = 0
        with pytest.raises((ValueError, Exception)):
            validate_yelp_review_record(record)

    def test_stars_out_of_range_high_raises(self):
        record = self._valid_record()
        record["stars"] = 6
        with pytest.raises((ValueError, Exception)):
            validate_yelp_review_record(record)

    def test_extra_fields_preserved(self):
        record = self._valid_record()
        record["extra"] = "extra_value"
        result = validate_yelp_review_record(record)
        assert "extra" in result
