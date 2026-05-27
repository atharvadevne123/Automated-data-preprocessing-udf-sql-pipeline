"""Tests for SentimentAnalyzer.analyze_stream()."""

from __future__ import annotations

import pytest

from pipeline.sentiment import SentimentAnalyzer, SentimentResult


@pytest.fixture
def analyzer() -> SentimentAnalyzer:
    return SentimentAnalyzer()


class TestAnalyzeStream:
    def test_returns_iterator(self, analyzer):
        result = analyzer.analyze_stream(iter(["hello"]))
        import types

        assert isinstance(result, types.GeneratorType)

    def test_yields_sentiment_results(self, analyzer):
        texts = ["great!", "terrible", "okay"]
        results = list(analyzer.analyze_stream(iter(texts)))
        assert all(isinstance(r, SentimentResult) for r in results)

    def test_count_matches_input(self, analyzer):
        texts = ["a", "b", "c", "d", "e"]
        results = list(analyzer.analyze_stream(iter(texts)))
        assert len(results) == 5

    def test_empty_iterator_yields_nothing(self, analyzer):
        results = list(analyzer.analyze_stream(iter([])))
        assert results == []

    def test_order_preserved(self, analyzer):
        texts = ["good", "bad", "neutral"]
        results = list(analyzer.analyze_stream(iter(texts)))
        assert results[0].text == "good"
        assert results[1].text == "bad"
        assert results[2].text == "neutral"

    def test_stream_is_lazy(self, analyzer):
        call_count = 0

        def counting_iter():
            nonlocal call_count
            for text in ["a", "b", "c"]:
                call_count += 1
                yield text

        stream = analyzer.analyze_stream(counting_iter())
        assert call_count == 0
        next(stream)
        assert call_count == 1

    def test_each_result_has_label(self, analyzer):
        texts = ["wonderful experience", "awful", "fine"]
        for result in analyzer.analyze_stream(iter(texts)):
            assert result.label in ("positive", "negative", "neutral")

    def test_each_result_has_polarity(self, analyzer):
        texts = ["great", "bad"]
        for result in analyzer.analyze_stream(iter(texts)):
            assert isinstance(result.polarity, float)

    @pytest.mark.parametrize("text", ["", "   ", "\n"])
    def test_blank_texts_return_neutral(self, analyzer, text):
        results = list(analyzer.analyze_stream(iter([text])))
        assert results[0].label == "neutral"

    def test_large_stream(self, analyzer):
        texts = [f"review {i}" for i in range(100)]
        results = list(analyzer.analyze_stream(iter(texts)))
        assert len(results) == 100
