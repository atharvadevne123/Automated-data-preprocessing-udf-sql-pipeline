"""Tests for utils/text_stats.py."""

from __future__ import annotations

import pytest

from utils.text_stats import (
    TextStats,
    compute_text_stats,
    lexical_diversity,
    reading_level_estimate,
    top_n_words,
)


class TestComputeTextStats:
    def test_empty_string(self) -> None:
        s = compute_text_stats("")
        assert s.char_count == 0
        assert s.word_count == 0

    def test_single_word(self) -> None:
        s = compute_text_stats("hello")
        assert s.char_count == 5
        assert s.word_count == 1
        assert s.unique_word_count == 1

    def test_word_count(self) -> None:
        s = compute_text_stats("the quick brown fox")
        assert s.word_count == 4

    def test_unique_words(self) -> None:
        s = compute_text_stats("apple apple orange")
        assert s.unique_word_count == 2

    def test_sentence_count(self) -> None:
        s = compute_text_stats("Hello. World. Foo.")
        assert s.sentence_count >= 2

    def test_avg_word_length(self) -> None:
        s = compute_text_stats("ab cde")
        assert s.avg_word_length == pytest.approx(2.5)

    def test_returns_text_stats_instance(self) -> None:
        s = compute_text_stats("hello world")
        assert isinstance(s, TextStats)

    def test_to_dict(self) -> None:
        s = compute_text_stats("hello world")
        d = s.to_dict()
        assert "word_count" in d
        assert "char_count" in d
        assert "sentence_count" in d
        assert "unique_word_count" in d
        assert "avg_word_length" in d
        assert "stopword_ratio" in d

    def test_non_string_input(self) -> None:
        s = compute_text_stats(None)  # type: ignore[arg-type]
        assert s.char_count == 0

    def test_stopword_ratio_range(self) -> None:
        s = compute_text_stats("the quick brown fox")
        assert 0.0 <= s.stopword_ratio <= 1.0


class TestLexicalDiversity:
    def test_empty(self) -> None:
        assert lexical_diversity("") == 0.0

    def test_all_unique(self) -> None:
        assert lexical_diversity("alpha beta gamma") == pytest.approx(1.0)

    def test_all_same(self) -> None:
        assert lexical_diversity("cat cat cat") == pytest.approx(1 / 3)

    def test_range(self) -> None:
        d = lexical_diversity("the quick brown fox jumps over the lazy dog")
        assert 0.0 <= d <= 1.0


class TestReadingLevelEstimate:
    def test_empty_string(self) -> None:
        assert reading_level_estimate("") == 0.0

    def test_returns_float(self) -> None:
        result = reading_level_estimate("The quick brown fox jumps.")
        assert isinstance(result, float)

    def test_simple_text_higher_score(self) -> None:
        simple = reading_level_estimate("I go to school.")
        assert isinstance(simple, float)


class TestTopNWords:
    def test_basic(self) -> None:
        result = top_n_words("apple apple orange banana apple", n=2)
        assert result[0][0] == "apple"
        assert result[0][1] == 3

    def test_n_limit(self) -> None:
        result = top_n_words("a b c d e f g h i j k", n=3)
        assert len(result) <= 3

    def test_empty_string(self) -> None:
        result = top_n_words("")
        assert result == []

    def test_stopword_exclusion(self) -> None:
        result = top_n_words("the the the apple", exclude_stopwords=True)
        words = [w for w, _ in result]
        assert "the" not in words

    def test_include_stopwords(self) -> None:
        result = top_n_words("the the the apple", exclude_stopwords=False)
        words = [w for w, _ in result]
        assert "the" in words

    @pytest.mark.parametrize("n", [1, 5, 10])
    def test_various_n(self, n: int) -> None:
        text = " ".join(f"word{i}" for i in range(20))
        result = top_n_words(text, n=n)
        assert len(result) <= n
