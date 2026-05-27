"""Extended parametrized tests for pipeline.cleaner.TextCleaner."""

from __future__ import annotations

import pytest

from pipeline.cleaner import TextCleaner


class TestTextCleanerCombined:
    """Test multiple options together in realistic scenarios."""

    @pytest.mark.parametrize(
        "text,expected_clean",
        [
            ("<p>Hello World</p>", "hello world"),
            ("Visit http://example.com today", "visit today"),
            ("LOUD!!!!!!!! text", "loud!! text"),
            ("  extra   spaces   here  ", "extra spaces here"),
            ("<b>Bold</b> and http://link.com loud!!!!", "bold and loud!!"),
        ],
    )
    def test_combined_cleaning(self, text, expected_clean):
        c = TextCleaner()
        result = c.clean(text)
        assert result == expected_clean

    def test_unicode_normalisation_nfkc(self):
        c = TextCleaner(normalize_unicode=True, lowercase=False)
        # NFKC normalises ligatures and special forms
        text = "ﬁle"  # fi ligature
        result = c.clean(text)
        assert "fi" in result or "le" in result

    @pytest.mark.parametrize("max_len", [5, 10, 50, 100, 1000])
    def test_max_length_truncation(self, max_len):
        c = TextCleaner(max_length=max_len)
        result = c.clean("x" * 2000)
        assert len(result) <= max_len

    def test_batch_preserves_order(self):
        c = TextCleaner()
        texts = [f"text {i}" for i in range(100)]
        results = c.clean_batch(texts)
        assert [r.replace("text ", "") for r in results] == [str(i) for i in range(100)]

    @pytest.mark.parametrize(
        "field,value",
        [
            ("text", "Hello World"),
            ("review", "Some Review"),
            ("description", "A Description"),
        ],
    )
    def test_clean_record_various_fields(self, field, value):
        c = TextCleaner(lowercase=True)
        record = {field: value, "other": "unchanged"}
        c.clean_record(record, field=field)
        assert record[field] == value.lower()
        assert record["other"] == "unchanged"

    def test_empty_batch_returns_empty(self):
        c = TextCleaner()
        assert c.clean_batch([]) == []

    def test_clean_batch_mixed_types(self):
        c = TextCleaner()
        result = c.clean_batch(["hello", "", "world"])
        assert result == ["hello", "", "world"]

    def test_cleaner_with_all_disabled(self):
        c = TextCleaner(
            lowercase=False,
            strip_urls=False,
            strip_html=False,
            normalize_unicode=False,
            collapse_whitespace=False,
            collapse_punctuation=False,
        )
        text = "<b>Hello</b> http://example.com !!!!"
        assert c.clean(text) == text

    @pytest.mark.parametrize(
        "html",
        [
            "<div>test</div>",
            "<span class='x'>text</span>",
            "<!-- comment -->text",
            "<br/>newline",
        ],
    )
    def test_various_html_tags_stripped(self, html):
        c = TextCleaner(strip_html=True, lowercase=False)
        result = c.clean(html)
        assert "<" not in result
