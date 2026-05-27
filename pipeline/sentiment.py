"""TextBlob-based sentiment analysis for Yelp review text."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterator

logger = logging.getLogger(__name__)


@dataclass
class SentimentResult:
    """Sentiment analysis result for a single text.

    Attributes:
        text: The original input text (first 200 chars for repr).
        polarity: Float in [-1.0, 1.0]; negative = negative sentiment.
        subjectivity: Float in [0.0, 1.0]; 0 = objective, 1 = subjective.
        label: One of ``'positive'``, ``'neutral'``, or ``'negative'``.
    """

    text: str
    polarity: float
    subjectivity: float
    label: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict."""
        return {
            "polarity": round(self.polarity, 4),
            "subjectivity": round(self.subjectivity, 4),
            "label": self.label,
        }


class SentimentAnalyzer:
    """Wrapper around TextBlob for batch sentiment analysis.

    Falls back gracefully if TextBlob is not installed — returns neutral
    results with a warning rather than crashing the pipeline.

    Args:
        positive_threshold: Polarity above this is labelled *positive* (default 0.1).
        negative_threshold: Polarity below this is labelled *negative* (default -0.1).
    """

    def __init__(
        self,
        positive_threshold: float = 0.1,
        negative_threshold: float = -0.1,
    ) -> None:
        if positive_threshold <= negative_threshold:
            raise ValueError("positive_threshold must be greater than negative_threshold")
        self._pos_thresh = positive_threshold
        self._neg_thresh = negative_threshold
        self._textblob_available = self._check_textblob()

    @staticmethod
    def _check_textblob() -> bool:
        try:
            import textblob  # noqa: F401

            return True
        except ImportError:
            logger.warning("textblob not installed — sentiment analysis will return neutral.")
            return False

    def _label(self, polarity: float) -> str:
        if polarity > self._pos_thresh:
            return "positive"
        if polarity < self._neg_thresh:
            return "negative"
        return "neutral"

    def analyze(self, text: str) -> SentimentResult:
        """Analyse *text* and return a SentimentResult.

        Args:
            text: Input text to analyse.

        Returns:
            SentimentResult with polarity, subjectivity, and label.
        """
        if not text or not self._textblob_available:
            return SentimentResult(text=text, polarity=0.0, subjectivity=0.0, label="neutral")
        try:
            from textblob import TextBlob

            blob = TextBlob(text)
            polarity = float(blob.sentiment.polarity)
            subjectivity = float(blob.sentiment.subjectivity)
            return SentimentResult(
                text=text,
                polarity=polarity,
                subjectivity=subjectivity,
                label=self._label(polarity),
            )
        except Exception as exc:
            logger.error("Sentiment analysis failed: %s", exc)
            return SentimentResult(text=text, polarity=0.0, subjectivity=0.0, label="neutral")

    def analyze_batch(self, texts: list[str]) -> list[SentimentResult]:
        """Analyse a list of texts and return a list of SentimentResults.

        Args:
            texts: List of input strings.

        Returns:
            List of SentimentResult objects in the same order.
        """
        return [self.analyze(t) for t in texts]

    def analyze_batch_parallel(
        self,
        texts: list[str],
        max_workers: int = 4,
    ) -> list[SentimentResult]:
        """Analyse texts concurrently using threads and preserve input order.

        Args:
            texts: List of input strings to analyse.
            max_workers: Number of worker threads (default 4).

        Returns:
            List of SentimentResult objects in the same order as *texts*.
        """
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(self.analyze, texts))

    def analyze_stream(
        self,
        texts: Iterator[str],
    ) -> Iterator[SentimentResult]:
        """Analyse texts from an iterator lazily, one at a time.

        Unlike :meth:`analyze_batch`, this does not materialise the full input
        into memory — suitable for large streaming datasets.

        Args:
            texts: Iterator of input strings.

        Yields:
            SentimentResult for each text in order.
        """
        for text in texts:
            yield self.analyze(text)

    def enrich_record(self, record: dict[str, Any], text_field: str = "text") -> dict[str, Any]:
        """Add sentiment fields to a record dict in-place and return it.

        Adds ``sentiment_polarity``, ``sentiment_subjectivity``, and
        ``sentiment_label`` keys to *record*.

        Args:
            record: Dict containing the text to analyse.
            text_field: Key in *record* holding the review text.

        Returns:
            The same dict with sentiment keys added.
        """
        text = record.get(text_field, "") or ""
        result = self.analyze(text)
        record["sentiment_polarity"] = result.polarity
        record["sentiment_subjectivity"] = result.subjectivity
        record["sentiment_label"] = result.label
        return record
