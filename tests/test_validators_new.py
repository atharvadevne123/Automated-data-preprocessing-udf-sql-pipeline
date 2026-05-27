"""Tests for new validators: validate_star_rating, validate_email, sanitize_text, validate_yelp_review_record."""

from __future__ import annotations

import pytest

from utils.validators import (
    ValidationError,
    sanitize_text,
    validate_email,
    validate_star_rating,
    validate_yelp_review_record,
)


class TestValidateStarRating:
    @pytest.mark.parametrize("stars", [1.0, 2.0, 3.0, 4.0, 5.0, 1, 5, 3.5])
    def test_valid_ratings(self, stars):
        result = validate_star_rating(stars)
        assert 1.0 <= result <= 5.0

    @pytest.mark.parametrize("stars", [0.0, 0.9, 5.1, 6.0, -1.0])
    def test_out_of_range_raises(self, stars):
        with pytest.raises(ValidationError):
            validate_star_rating(stars)

    @pytest.mark.parametrize("stars", ["abc", None, [], {}])
    def test_non_numeric_raises(self, stars):
        with pytest.raises(ValidationError):
            validate_star_rating(stars)

    def test_returns_float(self):
        assert isinstance(validate_star_rating(4), float)

    def test_boundary_one(self):
        assert validate_star_rating(1) == 1.0

    def test_boundary_five(self):
        assert validate_star_rating(5) == 5.0


class TestValidateEmail:
    @pytest.mark.parametrize(
        "email",
        [
            "user@example.com",
            "name.last@domain.co.uk",
            "test+tag@sub.domain.org",
            " user@example.com ",
        ],
    )
    def test_valid_emails(self, email):
        result = validate_email(email)
        assert "@" in result

    @pytest.mark.parametrize(
        "email",
        [
            "",
            "notanemail",
            "@nodomain.com",
            "user@",
            "user@domain",
            None,
        ],
    )
    def test_invalid_emails_raise(self, email):
        with pytest.raises((ValidationError, AttributeError)):
            validate_email(email or "")

    def test_strips_whitespace(self):
        result = validate_email("  user@example.com  ")
        assert result == "user@example.com"


class TestSanitizeText:
    def test_strips_whitespace(self):
        assert sanitize_text("  hello  ") == "hello"

    def test_removes_null_bytes(self):
        result = sanitize_text("hello\x00world")
        assert "\x00" not in result

    def test_truncates_to_max_length(self):
        result = sanitize_text("a" * 200, max_length=50)
        assert len(result) == 50

    def test_no_truncation_if_short(self):
        result = sanitize_text("short", max_length=100)
        assert result == "short"

    def test_non_string_raises(self):
        with pytest.raises(ValidationError):
            sanitize_text(123)  # type: ignore[arg-type]

    def test_empty_string_returns_empty(self):
        assert sanitize_text("") == ""

    @pytest.mark.parametrize("text", ["hello", "world", "foo bar"])
    def test_valid_texts_returned_unchanged(self, text):
        assert sanitize_text(text) == text


class TestValidateYelpReviewRecord:
    def _valid(self):
        return {"review_id": "r1", "user_id": "u1", "business_id": "b1", "stars": 4.0}

    def test_valid_record_passes(self):
        record = self._valid()
        result = validate_yelp_review_record(record)
        assert result is record

    def test_missing_review_id_raises(self):
        r = self._valid()
        del r["review_id"]
        with pytest.raises(ValidationError):
            validate_yelp_review_record(r)

    def test_missing_stars_raises(self):
        r = self._valid()
        del r["stars"]
        with pytest.raises(ValidationError):
            validate_yelp_review_record(r)

    def test_invalid_stars_raises(self):
        r = self._valid()
        r["stars"] = 6.0
        with pytest.raises(ValidationError):
            validate_yelp_review_record(r)

    def test_extra_fields_allowed(self):
        r = self._valid()
        r["extra"] = "data"
        result = validate_yelp_review_record(r)
        assert result["extra"] == "data"
