"""Text cleaning and normalisation utilities for Yelp review text."""

from __future__ import annotations

import logging
import re
import string
import unicodedata
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Pre-compiled patterns for performance
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"\s+")
_PUNCTUATION_REPEAT_RE = re.compile(r"([!?.]){3,}")
_PUNCTUATION_RE = re.compile(r"[" + re.escape(string.punctuation) + r"]")
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_PHONE_RE = re.compile(r"\+?[\d][\d\s\-().]{5,}\d")


@dataclass
class CleanerStats:
    """Tracks statistics for a TextCleaner session.

    Attributes:
        total_cleaned: Number of texts processed.
        empty_inputs: Texts that were empty or non-string.
        chars_removed: Total characters removed across all texts.
        truncated: Texts truncated due to max_length.
    """

    total_cleaned: int = 0
    empty_inputs: int = 0
    chars_removed: int = 0
    truncated: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary."""
        return {
            "total_cleaned": self.total_cleaned,
            "empty_inputs": self.empty_inputs,
            "chars_removed": self.chars_removed,
            "truncated": self.truncated,
        }

    def reset(self) -> None:
        """Reset all counters to zero."""
        self.total_cleaned = 0
        self.empty_inputs = 0
        self.chars_removed = 0
        self.truncated = 0


class TextCleaner:
    """Clean and normalise raw review text for downstream NLP tasks.

    Each cleaning step is individually toggleable.

    Args:
        lowercase: Convert text to lowercase (default True).
        strip_urls: Remove http/https URLs and www links (default True).
        strip_html: Remove HTML tags (default True).
        normalize_unicode: Apply NFKC unicode normalisation (default True).
        collapse_whitespace: Replace runs of whitespace with a single space (default True).
        collapse_punctuation: Reduce repeated ``!?!?...`` to max 2 (default True).
        strip_punctuation: Remove all punctuation characters (default False).
        max_length: Truncate text to this many characters; None for no limit (default None).
        track_stats: Accumulate cleaning statistics in ``self.stats`` (default False).
    """

    def __init__(
        self,
        lowercase: bool = True,
        strip_urls: bool = True,
        strip_html: bool = True,
        normalize_unicode: bool = True,
        collapse_whitespace: bool = True,
        collapse_punctuation: bool = True,
        strip_punctuation: bool = False,
        max_length: int | None = None,
        track_stats: bool = False,
    ) -> None:
        self.lowercase = lowercase
        self.strip_urls = strip_urls
        self.strip_html = strip_html
        self.normalize_unicode = normalize_unicode
        self.collapse_whitespace = collapse_whitespace
        self.collapse_punctuation = collapse_punctuation
        self.strip_punctuation = strip_punctuation
        self.max_length = max_length
        self.track_stats = track_stats
        self.stats: CleanerStats = CleanerStats()

    def clean(self, text: str) -> str:
        """Apply all enabled cleaning steps to *text*.

        Args:
            text: Raw input string.

        Returns:
            Cleaned string. Returns ``""`` for non-string or empty input.
        """
        if not isinstance(text, str) or not text:
            if self.track_stats:
                self.stats.empty_inputs += 1
            return ""
        original_len = len(text)
        if self.normalize_unicode:
            text = unicodedata.normalize("NFKC", text)
        if self.strip_html:
            text = _HTML_TAG_RE.sub(" ", text)
        if self.strip_urls:
            text = _URL_RE.sub(" ", text)
        if self.collapse_punctuation:
            text = _PUNCTUATION_REPEAT_RE.sub(r"\1\1", text)
        if self.strip_punctuation:
            text = _PUNCTUATION_RE.sub(" ", text)
        if self.lowercase:
            text = text.lower()
        if self.collapse_whitespace:
            text = _MULTI_SPACE_RE.sub(" ", text).strip()
        if self.max_length is not None and len(text) > self.max_length:
            text = text[: self.max_length]
            if self.track_stats:
                self.stats.truncated += 1
        if self.track_stats:
            self.stats.total_cleaned += 1
            self.stats.chars_removed += max(0, original_len - len(text))
        return text

    def clean_record(self, record: dict[str, Any], field: str = "text") -> dict[str, Any]:
        """Clean the text in *record[field]* in-place.

        Args:
            record: Dict containing the text field.
            field: Key of the text to clean.

        Returns:
            The same dict with the cleaned text value.
        """
        if field in record:
            record[field] = self.clean(str(record[field]))
        return record

    def clean_batch(self, texts: list[str]) -> list[str]:
        """Clean a list of texts.

        Args:
            texts: Input strings.

        Returns:
            List of cleaned strings in the same order.
        """
        return [self.clean(t) for t in texts]

    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Collapse all runs of whitespace (including tabs/newlines) to a single space.

        Does not apply any other cleaning steps.  Strips leading and trailing
        whitespace from the result.

        Args:
            text: Input string.

        Returns:
            String with normalised whitespace, or ``""`` for non-string input.
        """
        if not isinstance(text, str):
            return ""
        return _MULTI_SPACE_RE.sub(" ", text).strip()

    @staticmethod
    def extract_emails(text: str) -> list[str]:
        """Return all email addresses found in *text*.

        Args:
            text: Input string.

        Returns:
            List of matched email strings (may be empty).
        """
        if not isinstance(text, str):
            return []
        return _EMAIL_RE.findall(text)

    @staticmethod
    def extract_phones(text: str) -> list[str]:
        """Return all phone-number-like substrings found in *text*.

        The pattern matches international and North-American formats.

        Args:
            text: Input string.

        Returns:
            List of matched phone strings (may be empty).
        """
        if not isinstance(text, str):
            return []
        return _PHONE_RE.findall(text)
