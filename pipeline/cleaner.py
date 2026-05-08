"""Text cleaning and normalisation utilities for Yelp review text."""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any

logger = logging.getLogger(__name__)

# Pre-compiled patterns for performance
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"\s+")
_PUNCTUATION_REPEAT_RE = re.compile(r"([!?.]){3,}")


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
        max_length: Truncate text to this many characters; None for no limit (default None).
    """

    def __init__(
        self,
        lowercase: bool = True,
        strip_urls: bool = True,
        strip_html: bool = True,
        normalize_unicode: bool = True,
        collapse_whitespace: bool = True,
        collapse_punctuation: bool = True,
        max_length: int | None = None,
    ) -> None:
        self.lowercase = lowercase
        self.strip_urls = strip_urls
        self.strip_html = strip_html
        self.normalize_unicode = normalize_unicode
        self.collapse_whitespace = collapse_whitespace
        self.collapse_punctuation = collapse_punctuation
        self.max_length = max_length

    def clean(self, text: str) -> str:
        """Apply all enabled cleaning steps to *text*.

        Args:
            text: Raw input string.

        Returns:
            Cleaned string. Returns ``""`` for non-string or empty input.
        """
        if not isinstance(text, str) or not text:
            return ""
        if self.normalize_unicode:
            text = unicodedata.normalize("NFKC", text)
        if self.strip_html:
            text = _HTML_TAG_RE.sub(" ", text)
        if self.strip_urls:
            text = _URL_RE.sub(" ", text)
        if self.collapse_punctuation:
            text = _PUNCTUATION_REPEAT_RE.sub(r"\1\1", text)
        if self.lowercase:
            text = text.lower()
        if self.collapse_whitespace:
            text = _MULTI_SPACE_RE.sub(" ", text).strip()
        if self.max_length is not None:
            text = text[: self.max_length]
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
