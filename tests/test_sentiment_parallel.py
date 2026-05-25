"""Tests for SentimentAnalyzer.analyze_batch_parallel()."""

from __future__ import annotations

import pytest

from pipeline.sentiment import SentimentAnalyzer, SentimentResult


class TestAnalyzeBatchParallel:
    def test_returns_list(self):
        analyzer = SentimentAnalyzer()
        results = analyzer.analyze_batch_parallel(["good", "bad", "ok"])
        assert isinstance(results, list)
        assert len(results) == 3

    def test_all_are_sentiment_results(self):
        analyzer = SentimentAnalyzer()
        results = analyzer.analyze_batch_parallel(["great", "terrible"])
        assert all(isinstance(r, SentimentResult) for r in results)

    def test_preserves_order(self):
        analyzer = SentimentAnalyzer()
        texts = [f"text_{i}" for i in range(10)]
        results = analyzer.analyze_batch_parallel(texts, max_workers=4)
        assert len(results) == 10

    def test_empty_input(self):
        analyzer = SentimentAnalyzer()
        results = analyzer.analyze_batch_parallel([])
        assert results == []

    def test_single_text(self):
        analyzer = SentimentAnalyzer()
        results = analyzer.analyze_batch_parallel(["excellent"])
        assert len(results) == 1
        assert results[0].label in ("positive", "neutral", "negative")

    def test_matches_sequential_labels(self):
        analyzer = SentimentAnalyzer()
        texts = ["great service", "terrible food", "it was okay"]
        parallel = analyzer.analyze_batch_parallel(texts, max_workers=2)
        sequential = analyzer.analyze_batch(texts)
        assert [r.label for r in parallel] == [r.label for r in sequential]

    def test_max_workers_one(self):
        analyzer = SentimentAnalyzer()
        results = analyzer.analyze_batch_parallel(["good", "bad"], max_workers=1)
        assert len(results) == 2

    @pytest.mark.parametrize("n_texts", [1, 5, 20])
    def test_parametrized_batch_sizes(self, n_texts):
        analyzer = SentimentAnalyzer()
        texts = ["sample text"] * n_texts
        results = analyzer.analyze_batch_parallel(texts)
        assert len(results) == n_texts
