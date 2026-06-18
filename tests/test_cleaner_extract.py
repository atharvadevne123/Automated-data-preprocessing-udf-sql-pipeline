"""Tests for TextCleaner.extract_emails() and extract_phones()."""

from __future__ import annotations

import pytest

from pipeline.cleaner import TextCleaner


class TestExtractEmails:
    def test_single_email(self) -> None:
        result = TextCleaner.extract_emails("Contact me at foo@bar.com for details.")
        assert "foo@bar.com" in result

    def test_multiple_emails(self) -> None:
        text = "a@b.com and c@d.org and e@f.net"
        result = TextCleaner.extract_emails(text)
        assert len(result) == 3

    def test_no_emails(self) -> None:
        assert TextCleaner.extract_emails("no emails here") == []

    def test_empty_string(self) -> None:
        assert TextCleaner.extract_emails("") == []

    def test_non_string(self) -> None:
        assert TextCleaner.extract_emails(None) == []  # type: ignore[arg-type]

    def test_email_with_plus(self) -> None:
        result = TextCleaner.extract_emails("test+tag@example.com")
        assert len(result) == 1

    def test_email_with_subdomain(self) -> None:
        result = TextCleaner.extract_emails("user@mail.example.co.uk")
        assert len(result) == 1

    @pytest.mark.parametrize(
        "text,count",
        [
            ("a@b.com", 1),
            ("no at sign", 0),
            ("x@y.org z@w.net", 2),
        ],
    )
    def test_parametrized(self, text: str, count: int) -> None:
        assert len(TextCleaner.extract_emails(text)) == count


class TestExtractPhones:
    def test_simple_phone(self) -> None:
        result = TextCleaner.extract_phones("Call 555-1234 today.")
        assert len(result) >= 1

    def test_no_phones(self) -> None:
        assert TextCleaner.extract_phones("no phone number") == []

    def test_empty_string(self) -> None:
        assert TextCleaner.extract_phones("") == []

    def test_non_string(self) -> None:
        assert TextCleaner.extract_phones(42) == []  # type: ignore[arg-type]

    def test_international_format(self) -> None:
        result = TextCleaner.extract_phones("+1 800 555 0100")
        assert len(result) >= 1

    @pytest.mark.parametrize(
        "text",
        [
            "Call us at 555-1234.",
            "Reach us at +1 (800) 555-0100.",
        ],
    )
    def test_common_formats(self, text: str) -> None:
        result = TextCleaner.extract_phones(text)
        assert len(result) >= 1
