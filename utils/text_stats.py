"""Text statistics utilities for Yelp review analysis."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_SENTENCE_END = re.compile(r"[.!?]+")
_WORD_BOUNDARY = re.compile(r"\s+")
_STOPWORDS = frozenset(
    "a an the and or but in on at to of for with is are was were be been being "
    "have has had do does did will would could should may might shall can".split()
)


@dataclass
class TextStats:
    """Statistical summary of a text string.

    Attributes:
        char_count: Total number of characters.
        word_count: Number of whitespace-delimited tokens.
        sentence_count: Approximate sentence count based on terminal punctuation.
        unique_word_count: Distinct lowercased word forms.
        avg_word_length: Mean characters per word token.
        stopword_ratio: Fraction of tokens that are common English stopwords.
    """

    char_count: int
    word_count: int
    sentence_count: int
    unique_word_count: int
    avg_word_length: float
    stopword_ratio: float

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary."""
        return {
            "char_count": self.char_count,
            "word_count": self.word_count,
            "sentence_count": self.sentence_count,
            "unique_word_count": self.unique_word_count,
            "avg_word_length": round(self.avg_word_length, 3),
            "stopword_ratio": round(self.stopword_ratio, 4),
        }


def compute_text_stats(text: str) -> TextStats:
    """Compute basic statistics for *text*.

    Args:
        text: Input string.  Empty string returns all-zero stats.

    Returns:
        A :class:`TextStats` dataclass instance.
    """
    if not text or not isinstance(text, str):
        return TextStats(0, 0, 0, 0, 0.0, 0.0)

    char_count = len(text)
    tokens = [t for t in _WORD_BOUNDARY.split(text.strip()) if t]
    word_count = len(tokens)
    sentences = [s for s in _SENTENCE_END.split(text) if s.strip()]
    sentence_count = max(1, len(sentences)) if tokens else 0
    lower_tokens = [t.lower() for t in tokens]
    unique_word_count = len(set(lower_tokens))
    avg_word_length = sum(len(t) for t in tokens) / word_count if word_count else 0.0
    stop_count = sum(1 for t in lower_tokens if t in _STOPWORDS)
    stopword_ratio = stop_count / word_count if word_count else 0.0

    return TextStats(
        char_count=char_count,
        word_count=word_count,
        sentence_count=sentence_count,
        unique_word_count=unique_word_count,
        avg_word_length=avg_word_length,
        stopword_ratio=stopword_ratio,
    )


def lexical_diversity(text: str) -> float:
    """Return type-token ratio (unique words / total words).

    Args:
        text: Input string.

    Returns:
        Float in [0.0, 1.0], or 0.0 for empty input.
    """
    stats = compute_text_stats(text)
    if stats.word_count == 0:
        return 0.0
    return stats.unique_word_count / stats.word_count


def reading_level_estimate(text: str) -> float:
    """Estimate reading difficulty as a simple Flesch-Kincaid-like score.

    The score uses: 206.835 - 1.015*(words/sentences) - 84.6*(syllables/words).
    Syllables are approximated by vowel-group counting.

    Args:
        text: Input string.

    Returns:
        Flesch reading ease score (higher = easier).  Returns 0.0 for empty input.
    """
    stats = compute_text_stats(text)
    if stats.word_count == 0 or stats.sentence_count == 0:
        return 0.0
    vowel_re = re.compile(r"[aeiouAEIOU]+")
    tokens = [t for t in _WORD_BOUNDARY.split(text.strip()) if t]
    syllable_count = sum(len(vowel_re.findall(t)) or 1 for t in tokens)
    asl = stats.word_count / stats.sentence_count
    asw = syllable_count / stats.word_count
    return round(206.835 - 1.015 * asl - 84.6 * asw, 2)


def top_n_words(text: str, n: int = 10, exclude_stopwords: bool = True) -> list[tuple[str, int]]:
    """Return the *n* most frequent words in *text*.

    Args:
        text: Input string.
        n: Number of top words to return.
        exclude_stopwords: Skip common English stopwords (default True).

    Returns:
        List of (word, count) tuples sorted by count descending.
    """
    if not text or not isinstance(text, str):
        return []
    tokens = [t.lower().strip(".,!?;:\"'") for t in _WORD_BOUNDARY.split(text) if t]
    if exclude_stopwords:
        tokens = [t for t in tokens if t and t not in _STOPWORDS]
    freq: dict[str, int] = {}
    for t in tokens:
        freq[t] = freq.get(t, 0) + 1
    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:n]
