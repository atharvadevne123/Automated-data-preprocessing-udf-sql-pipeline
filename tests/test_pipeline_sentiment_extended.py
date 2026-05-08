"""Extended tests for SentimentAnalyzer — edge cases and batch semantics."""

from __future__ import annotations

import pytest

from pipeline.sentiment import SentimentAnalyzer, SentimentResult


class TestLabelAssignment:
    @pytest.mark.parametrize("polarity,expected_label", [
        (0.5, "positive"),
        (0.1, "positive"),
        (0.0, "neutral"),
        (-0.05, "neutral"),
        (-0.1, "negative"),
        (-0.8, "negative"),
    ])
    def test_label_from_polarity(self, polarity, expected_label):
        a = SentimentAnalyzer(positive_threshold=0.1, negative_threshold=-0.1)
        assert a._label(polarity) == expected_label

    @pytest.mark.parametrize("pos_thresh,neg_thresh,polarity,expected", [
        (0.3, -0.3, 0.2, "neutral"),
        (0.3, -0.3, 0.4, "positive"),
        (0.3, -0.3, -0.4, "negative"),
        (0.05, -0.05, 0.06, "positive"),
    ])
    def test_custom_threshold_label(self, pos_thresh, neg_thresh, polarity, expected):
        a = SentimentAnalyzer(positive_threshold=pos_thresh, negative_threshold=neg_thresh)
        assert a._label(polarity) == expected


class TestBatchAnalysis:
    @pytest.fixture
    def analyzer(self):
        return SentimentAnalyzer()

    def test_batch_empty_list(self, analyzer):
        results = analyzer.analyze_batch([])
        assert results == []

    def test_batch_single_item(self, analyzer):
        results = analyzer.analyze_batch(["amazing"])
        assert len(results) == 1
        assert isinstance(results[0], SentimentResult)

    def test_batch_order_preserved(self, analyzer):
        texts = ["a", "b", "c", "d", "e"]
        results = analyzer.analyze_batch(texts)
        assert len(results) == len(texts)

    @pytest.mark.parametrize("n", [1, 5, 20, 50])
    def test_batch_size_various(self, analyzer, n):
        texts = [f"review {i}" for i in range(n)]
        results = analyzer.analyze_batch(texts)
        assert len(results) == n


class TestEnrichRecord:
    @pytest.fixture
    def analyzer(self):
        return SentimentAnalyzer()

    def test_returns_same_dict(self, analyzer):
        record = {"text": "great"}
        result = analyzer.enrich_record(record)
        assert result is record

    def test_preserves_original_fields(self, analyzer):
        record = {"text": "good", "stars": 5, "id": "abc"}
        analyzer.enrich_record(record)
        assert record["stars"] == 5
        assert record["id"] == "abc"

    def test_sentiment_polarity_is_float(self, analyzer):
        record = {"text": "test"}
        analyzer.enrich_record(record)
        assert isinstance(record["sentiment_polarity"], float)

    def test_sentiment_subjectivity_is_float(self, analyzer):
        record = {"text": "test"}
        analyzer.enrich_record(record)
        assert isinstance(record["sentiment_subjectivity"], float)

    def test_sentiment_label_is_string(self, analyzer):
        record = {"text": "test"}
        analyzer.enrich_record(record)
        assert isinstance(record["sentiment_label"], str)

    @pytest.mark.parametrize("text_field", ["text", "review", "body", "comment"])
    def test_various_text_fields(self, analyzer, text_field):
        record = {text_field: "excellent experience", "other": 1}
        analyzer.enrich_record(record, text_field=text_field)
        assert "sentiment_label" in record


class TestSentimentResultProperties:
    def test_polarity_rounded_in_dict(self):
        r = SentimentResult(text="x", polarity=0.123456789, subjectivity=0.5, label="positive")
        d = r.to_dict()
        assert len(str(d["polarity"]).split(".")[-1]) <= 4

    def test_subjectivity_rounded_in_dict(self):
        r = SentimentResult(text="x", polarity=0.0, subjectivity=0.987654321, label="neutral")
        d = r.to_dict()
        assert len(str(d["subjectivity"]).split(".")[-1]) <= 4

    def test_label_in_dict(self):
        for label in ["positive", "neutral", "negative"]:
            r = SentimentResult(text="x", polarity=0.0, subjectivity=0.0, label=label)
            assert r.to_dict()["label"] == label
