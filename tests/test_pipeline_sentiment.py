"""Tests for pipeline.sentiment.SentimentAnalyzer and SentimentResult."""

from __future__ import annotations

import pytest

from pipeline.sentiment import SentimentAnalyzer, SentimentResult


class TestSentimentAnalyzerInit:
    def test_default_thresholds(self):
        s = SentimentAnalyzer()
        assert s._pos_thresh == 0.1
        assert s._neg_thresh == -0.1

    def test_custom_thresholds(self):
        s = SentimentAnalyzer(positive_threshold=0.3, negative_threshold=-0.3)
        assert s._pos_thresh == 0.3

    def test_invalid_thresholds_raises(self):
        with pytest.raises(ValueError):
            SentimentAnalyzer(positive_threshold=0.1, negative_threshold=0.2)

    def test_equal_thresholds_raises(self):
        with pytest.raises(ValueError):
            SentimentAnalyzer(positive_threshold=0.0, negative_threshold=0.0)


class TestSentimentAnalyzerAnalyze:
    @pytest.fixture
    def analyzer(self):
        return SentimentAnalyzer()

    def test_empty_text_returns_neutral(self, analyzer):
        result = analyzer.analyze("")
        assert result.label == "neutral"
        assert result.polarity == 0.0

    def test_returns_sentiment_result(self, analyzer):
        result = analyzer.analyze("The food was amazing!")
        assert isinstance(result, SentimentResult)

    def test_label_in_valid_values(self, analyzer):
        result = analyzer.analyze("some text")
        assert result.label in ("positive", "neutral", "negative")

    def test_polarity_range(self, analyzer):
        result = analyzer.analyze("Great experience")
        assert -1.0 <= result.polarity <= 1.0

    def test_subjectivity_range(self, analyzer):
        result = analyzer.analyze("Good food")
        assert 0.0 <= result.subjectivity <= 1.0

    @pytest.mark.parametrize("text", ["", "  ", None])
    def test_empty_or_none_text(self, analyzer, text):
        result = analyzer.analyze(text or "")
        assert result.label == "neutral"


class TestSentimentResultToDict:
    def test_to_dict_keys(self):
        r = SentimentResult(text="hello", polarity=0.5, subjectivity=0.3, label="positive")
        d = r.to_dict()
        assert "polarity" in d
        assert "subjectivity" in d
        assert "label" in d

    def test_to_dict_values(self):
        r = SentimentResult(text="x", polarity=0.1234, subjectivity=0.5678, label="positive")
        d = r.to_dict()
        assert d["polarity"] == round(0.1234, 4)
        assert d["label"] == "positive"


class TestSentimentAnalyzerBatch:
    def test_batch_returns_correct_length(self):
        analyzer = SentimentAnalyzer()
        texts = ["good", "bad", "okay", ""]
        results = analyzer.analyze_batch(texts)
        assert len(results) == 4

    def test_batch_all_sentiment_results(self):
        analyzer = SentimentAnalyzer()
        results = analyzer.analyze_batch(["a", "b"])
        assert all(isinstance(r, SentimentResult) for r in results)


class TestSentimentAnalyzerEnrichRecord:
    def test_enrich_adds_keys(self):
        analyzer = SentimentAnalyzer()
        record = {"text": "great food", "stars": 5}
        enriched = analyzer.enrich_record(record)
        assert "sentiment_label" in enriched
        assert "sentiment_polarity" in enriched
        assert "sentiment_subjectivity" in enriched

    def test_enrich_custom_field(self):
        analyzer = SentimentAnalyzer()
        record = {"review": "good stuff"}
        analyzer.enrich_record(record, text_field="review")
        assert "sentiment_label" in record

    def test_enrich_missing_field_returns_neutral(self):
        analyzer = SentimentAnalyzer()
        record = {}
        analyzer.enrich_record(record)
        assert record["sentiment_label"] == "neutral"

    @pytest.mark.parametrize("label_key", ["sentiment_label", "sentiment_polarity", "sentiment_subjectivity"])
    def test_enrich_all_keys_present(self, label_key):
        analyzer = SentimentAnalyzer()
        record = {"text": "test"}
        analyzer.enrich_record(record)
        assert label_key in record
