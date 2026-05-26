"""Tests for new TextCleaner features: strip_punctuation, CleanerStats."""

from __future__ import annotations

import pytest

from pipeline.cleaner import TextCleaner


class TestStripPunctuation:
    def test_removes_punctuation(self):
        cleaner = TextCleaner(strip_punctuation=True, lowercase=False, collapse_whitespace=True)
        result = cleaner.clean("Hello, world!")
        assert "," not in result
        assert "!" not in result
        assert "Hello" in result

    def test_default_no_strip(self):
        cleaner = TextCleaner(strip_punctuation=False)
        result = cleaner.clean("Hello, world!")
        assert "," in result

    def test_strip_and_lowercase(self):
        cleaner = TextCleaner(strip_punctuation=True, lowercase=True)
        result = cleaner.clean("Great!! Food?")
        assert "!" not in result
        assert "?" not in result
        assert "great" in result

    @pytest.mark.parametrize("text,contains", [
        ("one,two", "onetwo"),
        ("a.b.c", "abc"),
    ])
    def test_parametrized_strip(self, text, contains):
        cleaner = TextCleaner(strip_punctuation=True, collapse_whitespace=True)
        result = cleaner.clean(text).replace(" ", "")
        for ch in contains:
            assert ch in result


class TestCleanerStats:
    def test_stats_disabled_by_default(self):
        cleaner = TextCleaner()
        cleaner.clean("hello")
        assert cleaner.stats.total_cleaned == 0

    def test_stats_enabled(self):
        cleaner = TextCleaner(track_stats=True)
        cleaner.clean("hello world")
        assert cleaner.stats.total_cleaned == 1

    def test_empty_input_counted(self):
        cleaner = TextCleaner(track_stats=True)
        cleaner.clean("")
        assert cleaner.stats.empty_inputs == 1
        assert cleaner.stats.total_cleaned == 0

    def test_chars_removed(self):
        cleaner = TextCleaner(track_stats=True, lowercase=False, strip_urls=False,
                              strip_html=False, collapse_whitespace=False)
        cleaner.clean("hello")
        assert cleaner.stats.chars_removed >= 0

    def test_truncated_counted(self):
        cleaner = TextCleaner(track_stats=True, max_length=3)
        cleaner.clean("hello world")
        assert cleaner.stats.truncated == 1

    def test_no_truncation_when_short(self):
        cleaner = TextCleaner(track_stats=True, max_length=100)
        cleaner.clean("hi")
        assert cleaner.stats.truncated == 0

    def test_to_dict_keys(self):
        cleaner = TextCleaner(track_stats=True)
        cleaner.clean("hello")
        d = cleaner.stats.to_dict()
        for key in ("total_cleaned", "empty_inputs", "chars_removed", "truncated"):
            assert key in d

    def test_reset(self):
        cleaner = TextCleaner(track_stats=True)
        cleaner.clean("hello")
        cleaner.stats.reset()
        assert cleaner.stats.total_cleaned == 0
        assert cleaner.stats.empty_inputs == 0

    def test_batch_tracking(self):
        cleaner = TextCleaner(track_stats=True)
        cleaner.clean_batch(["a", "b", "c", ""])
        assert cleaner.stats.total_cleaned == 3
        assert cleaner.stats.empty_inputs == 1

    @pytest.mark.parametrize("n", [1, 3, 5])
    def test_batch_count_parametrized(self, n):
        cleaner = TextCleaner(track_stats=True)
        cleaner.clean_batch(["hello"] * n)
        assert cleaner.stats.total_cleaned == n
