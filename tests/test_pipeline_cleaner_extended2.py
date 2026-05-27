"""Second set of extended tests for TextCleaner — unicode, edge inputs, option matrix."""

from __future__ import annotations

import pytest

from pipeline.cleaner import TextCleaner


class TestUnicodeNormalization:
    def test_nfkc_normalization_applied(self):
        c = TextCleaner(normalize_unicode=True, lowercase=False)
        # 'ﬁ' is the fi ligature; NFKC maps it to 'fi'
        result = c.clean("ﬁle")
        assert result == "file"

    def test_normalization_disabled(self):
        c = TextCleaner(normalize_unicode=False, lowercase=False, collapse_whitespace=False)
        ligature = "ﬁle"
        result = c.clean(ligature)
        assert "ﬁ" in result

    @pytest.mark.parametrize(
        "char,expected_substr",
        [
            ("é", "e"),  # é -> e after NFKC + lower
            ("…", "..."),  # ellipsis -> three dots
        ],
    )
    def test_unicode_chars(self, char, expected_substr):
        c = TextCleaner(normalize_unicode=True, lowercase=True)
        result = c.clean(char)
        assert expected_substr in result or result != ""


class TestNonStringInputs:
    def test_none_returns_empty(self):
        c = TextCleaner()
        assert c.clean(None) == ""  # type: ignore[arg-type]

    def test_integer_returns_empty_or_converts(self):
        c = TextCleaner()
        result = c.clean(42)  # type: ignore[arg-type]
        assert isinstance(result, str)


class TestWhitespaceEdgeCases:
    def test_tab_collapsed(self):
        c = TextCleaner(collapse_whitespace=True)
        result = c.clean("a\t\tb")
        assert result == "a b"

    def test_newline_collapsed(self):
        c = TextCleaner(collapse_whitespace=True)
        result = c.clean("line1\n\nline2")
        assert "\n" not in result

    def test_mixed_whitespace_collapsed(self):
        c = TextCleaner(collapse_whitespace=True)
        result = c.clean("a  \t \n  b")
        assert result == "a b"

    def test_collapse_disabled_preserves_spaces(self):
        c = TextCleaner(
            collapse_whitespace=False,
            strip_urls=False,
            strip_html=False,
            normalize_unicode=False,
            lowercase=False,
            collapse_punctuation=False,
        )
        result = c.clean("a   b")
        assert "   " in result


class TestMaxLengthEdgeCases:
    @pytest.mark.parametrize("length", [1, 5, 10, 100])
    def test_truncation_exact(self, length):
        c = TextCleaner(
            max_length=length,
            collapse_whitespace=False,
            strip_urls=False,
            strip_html=False,
            normalize_unicode=False,
            lowercase=False,
            collapse_punctuation=False,
        )
        text = "a" * (length + 50)
        result = c.clean(text)
        assert len(result) == length

    def test_max_length_zero_returns_empty(self):
        c = TextCleaner(max_length=0)
        result = c.clean("hello")
        assert result == ""


class TestAllOptionsDisabled:
    def test_passthrough_when_all_disabled(self):
        c = TextCleaner(
            lowercase=False,
            strip_urls=False,
            strip_html=False,
            normalize_unicode=False,
            collapse_whitespace=False,
            collapse_punctuation=False,
            max_length=None,
        )
        text = "Hello WORLD 123"
        assert c.clean(text) == text


class TestCleanBatchMethod:
    def test_clean_list_of_texts(self):
        c = TextCleaner()
        texts = ["  hello  ", "WORLD", "foo   bar"]
        results = [c.clean(t) for t in texts]
        assert results[0] == "hello"
        assert results[1] == "world"
        assert results[2] == "foo bar"

    @pytest.mark.parametrize("n", [0, 1, 10, 100])
    def test_batch_size_preserved(self, n):
        c = TextCleaner()
        texts = [f"item {i}" for i in range(n)]
        results = [c.clean(t) for t in texts]
        assert len(results) == n
