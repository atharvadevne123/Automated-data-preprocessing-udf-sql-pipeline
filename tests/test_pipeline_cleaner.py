"""Tests for pipeline.cleaner.TextCleaner."""

from __future__ import annotations

import pytest

from pipeline.cleaner import TextCleaner


@pytest.fixture
def cleaner():
    return TextCleaner()


class TestTextCleanerBasic:
    def test_empty_string_returns_empty(self, cleaner):
        assert cleaner.clean("") == ""

    def test_non_string_returns_empty(self, cleaner):
        assert cleaner.clean(None) == ""  # type: ignore[arg-type]

    def test_plain_text_unchanged_modulo_lower(self, cleaner):
        result = cleaner.clean("Hello World")
        assert result == "hello world"

    def test_lowercase_applied(self, cleaner):
        assert cleaner.clean("UPPERCASE") == "uppercase"

    def test_no_lowercase_when_disabled(self):
        c = TextCleaner(lowercase=False)
        assert c.clean("Hello") == "Hello"


class TestTextCleanerUrl:
    def test_strips_http_url(self, cleaner):
        result = cleaner.clean("Visit http://example.com today")
        assert "http" not in result

    def test_strips_https_url(self, cleaner):
        result = cleaner.clean("See https://www.yelp.com for details")
        assert "https" not in result

    def test_strips_www_url(self, cleaner):
        result = cleaner.clean("Check www.google.com out")
        assert "www.google.com" not in result

    def test_no_url_stripping_when_disabled(self):
        c = TextCleaner(strip_urls=False)
        result = c.clean("http://example.com")
        assert "http" in result


class TestTextCleanerHtml:
    def test_strips_html_tags(self, cleaner):
        result = cleaner.clean("<b>Bold</b> text")
        assert "<b>" not in result
        assert "bold" in result

    def test_strips_anchor_tags(self, cleaner):
        result = cleaner.clean('<a href="url">link</a>')
        assert "<a" not in result

    def test_no_html_stripping_when_disabled(self):
        c = TextCleaner(strip_html=False)
        result = c.clean("<b>text</b>")
        assert "<b>" in result


class TestTextCleanerWhitespace:
    def test_collapses_multiple_spaces(self, cleaner):
        result = cleaner.clean("too   many    spaces")
        assert "  " not in result

    def test_strips_leading_trailing_whitespace(self, cleaner):
        result = cleaner.clean("  leading and trailing  ")
        assert result == result.strip()

    def test_no_whitespace_collapse_when_disabled(self):
        c = TextCleaner(collapse_whitespace=False)
        result = c.clean("a  b")
        assert "  " in result


class TestTextCleanerPunctuation:
    def test_collapses_repeated_exclamation(self, cleaner):
        result = cleaner.clean("Wow!!!!!!!")
        assert "!!!" not in result

    def test_collapses_repeated_question(self, cleaner):
        result = cleaner.clean("Really????")
        assert "???" not in result

    def test_no_punct_collapse_when_disabled(self):
        c = TextCleaner(collapse_punctuation=False)
        result = c.clean("Wow!!!!!")
        assert "!!!" in result


class TestTextCleanerMaxLength:
    def test_truncates_long_text(self):
        c = TextCleaner(max_length=10)
        result = c.clean("a" * 50)
        assert len(result) <= 10

    def test_no_truncation_when_none(self, cleaner):
        text = "x" * 1000
        result = cleaner.clean(text)
        assert len(result) == 1000


class TestTextCleanerBatch:
    def test_batch_returns_correct_count(self, cleaner):
        texts = ["Hello World", "foo", "bar"]
        result = cleaner.clean_batch(texts)
        assert len(result) == 3

    def test_batch_applies_same_rules(self, cleaner):
        result = cleaner.clean_batch(["HELLO", "WORLD"])
        assert result == ["hello", "world"]


class TestTextCleanerRecord:
    def test_clean_record_modifies_text_field(self, cleaner):
        record = {"text": "HELLO WORLD", "stars": 5}
        result = cleaner.clean_record(record)
        assert result["text"] == "hello world"
        assert result["stars"] == 5

    def test_clean_record_custom_field(self, cleaner):
        record = {"review": "GREAT FOOD"}
        cleaner.clean_record(record, field="review")
        assert record["review"] == "great food"

    def test_clean_record_missing_field_unchanged(self, cleaner):
        record = {"stars": 4}
        cleaner.clean_record(record)
        assert "text" not in record

    @pytest.mark.parametrize(
        "text,expected_not_in",
        [
            ("http://example.com great food", "http"),
            ("<b>bold</b>", "<b>"),
            ("too   many   spaces", "  "),
        ],
    )
    def test_clean_removes_expected_content(self, cleaner, text, expected_not_in):
        result = cleaner.clean(text)
        assert expected_not_in not in result
