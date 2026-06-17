"""Field-value normalisation for Yelp pipeline records."""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_PHONE_STRIP = re.compile(r"[\s\-().+]")


class FieldNormalizer:
    """Apply normalisation functions to specific record fields.

    Normalisation functions are registered per field name and applied in order.

    Args:
        strict: If True, raise ``KeyError`` when a registered field is missing
            from a record.  If False (default), missing fields are skipped.

    Example::

        norm = FieldNormalizer()
        norm.register("stars", FieldNormalizer.to_float)
        norm.register("text", str.strip)
        normalised = norm.normalise(record)
    """

    def __init__(self, strict: bool = False) -> None:
        self._strict = strict
        self._rules: dict[str, list[Any]] = {}

    def register(self, field: str, func: Any) -> None:
        """Register a normalisation function for *field*.

        Multiple functions can be registered for the same field; they are
        applied in registration order.

        Args:
            field: Record key to normalise.
            func: Callable ``(value) -> value``.
        """
        self._rules.setdefault(field, []).append(func)

    def normalise(self, record: dict[str, Any]) -> dict[str, Any]:
        """Return a normalised copy of *record*.

        Args:
            record: Raw record dict.

        Returns:
            New dict with normalised values.  Fields not listed in rules are
            copied unchanged.

        Raises:
            KeyError: If *strict* is True and a registered field is absent.
        """
        result = dict(record)
        for field, funcs in self._rules.items():
            if field not in result:
                if self._strict:
                    raise KeyError(f"Field '{field}' missing from record.")
                continue
            value = result[field]
            for func in funcs:
                value = func(value)
            result[field] = value
        return result

    def normalise_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalise all records in *records*.

        Args:
            records: List of raw record dicts.

        Returns:
            List of normalised dicts.
        """
        out = [self.normalise(r) for r in records]
        logger.info("FieldNormalizer: normalised %d records.", len(out))
        return out

    @staticmethod
    def to_float(value: Any) -> float:
        """Coerce *value* to float, returning 0.0 on failure."""
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def to_int(value: Any) -> int:
        """Coerce *value* to int, returning 0 on failure."""
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def strip_whitespace(value: Any) -> str:
        """Return *value* stripped of leading/trailing whitespace."""
        return str(value).strip() if value is not None else ""

    @staticmethod
    def normalise_stars(value: Any) -> float:
        """Clamp a star rating to [1.0, 5.0] and coerce to float."""
        try:
            v = float(value)
        except (TypeError, ValueError):
            return 0.0
        return max(1.0, min(5.0, v))

    @staticmethod
    def normalise_phone(value: Any) -> str:
        """Strip formatting characters from a phone number string.

        Returns only digits, e.g. ``"+1 (800) 555-0100"`` → ``"18005550100"``.
        """
        if not isinstance(value, str):
            return ""
        return _PHONE_STRIP.sub("", value)

    @staticmethod
    def lowercase_strip(value: Any) -> str:
        """Lowercase and strip *value*."""
        return str(value).lower().strip() if value is not None else ""
