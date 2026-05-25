"""Record joining utilities for merging Yelp review and business data."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class RecordJoiner:
    """Join two collections of records on a shared key field.

    One collection is loaded as the *lookup* (right side); the other is
    streamed as the *main* records (left side).  The result is a left join:
    every main record is returned, enriched with matching lookup fields where
    available.

    Args:
        lookup_key: Field name used to match records in the lookup table.
        main_key: Field name in main records used for matching (defaults to
                  *lookup_key* if not specified).
        prefix: String prepended to lookup field names in the output to avoid
                collisions (default ``""`` means no prefix).

    Example::

        joiner = RecordJoiner(lookup_key="business_id", prefix="biz_")
        joiner.load_lookup(businesses)  # list of business dicts
        enriched = joiner.join(reviews)
    """

    def __init__(
        self,
        lookup_key: str,
        main_key: str | None = None,
        prefix: str = "",
    ) -> None:
        self._lookup_key = lookup_key
        self._main_key = main_key or lookup_key
        self._prefix = prefix
        self._index: dict[Any, dict[str, Any]] = {}

    def load_lookup(self, records: list[dict[str, Any]]) -> int:
        """Build the lookup index from *records*.

        Args:
            records: List of dicts that constitute the right side of the join.

        Returns:
            Number of records indexed.
        """
        self._index.clear()
        for record in records:
            key = record.get(self._lookup_key)
            if key is not None:
                self._index[key] = record
        logger.info("RecordJoiner: indexed %d lookup records on %r", len(self._index), self._lookup_key)
        return len(self._index)

    def join_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Enrich a single *record* with lookup fields.

        Fields from the matching lookup entry are merged into a copy of
        *record*.  Lookup fields are optionally prefixed to avoid overwriting
        existing keys.

        Args:
            record: Main record to enrich.

        Returns:
            New dict combining main and (prefixed) lookup fields.
        """
        result = dict(record)
        key = record.get(self._main_key)
        lookup_entry = self._index.get(key)
        if lookup_entry:
            for field, value in lookup_entry.items():
                if field == self._lookup_key:
                    continue
                out_field = f"{self._prefix}{field}" if self._prefix else field
                result.setdefault(out_field, value)
        return result

    def join(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Join all *records* against the loaded lookup index.

        Args:
            records: Main records to enrich.

        Returns:
            List of enriched dicts in the same order as *records*.
        """
        return [self.join_record(r) for r in records]

    def join_unmatched(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return only records from *records* that had no match in the lookup.

        Args:
            records: Main records to filter.

        Returns:
            Subset of *records* with no matching lookup entry.
        """
        return [r for r in records if r.get(self._main_key) not in self._index]

    @property
    def lookup_size(self) -> int:
        """Number of entries in the lookup index."""
        return len(self._index)
