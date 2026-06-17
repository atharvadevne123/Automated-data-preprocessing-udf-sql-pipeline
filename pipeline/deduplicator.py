"""Hash-based record deduplication for JSONL pipeline records."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Iterator

logger = logging.getLogger(__name__)


def _record_hash(record: dict[str, Any], key_fields: list[str] | None) -> str:
    """Compute a stable SHA-256 hex digest for *record*.

    Args:
        record: Dict to hash.
        key_fields: If given, only these fields contribute to the hash.
            Otherwise the full sorted record is used.

    Returns:
        Lowercase hex digest string.
    """
    if key_fields is not None:
        payload = {k: record.get(k) for k in sorted(key_fields)}
    else:
        payload = dict(sorted(record.items()))
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


class DeduplicationStats:
    """Counters for a deduplication pass.

    Attributes:
        total_seen: Records examined.
        duplicates_dropped: Records dropped as duplicates.
    """

    def __init__(self) -> None:
        self.total_seen: int = 0
        self.duplicates_dropped: int = 0

    @property
    def unique_count(self) -> int:
        """Records kept after deduplication."""
        return self.total_seen - self.duplicates_dropped

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-serialisable summary."""
        return {
            "total_seen": self.total_seen,
            "duplicates_dropped": self.duplicates_dropped,
            "unique_count": self.unique_count,
        }

    def reset(self) -> None:
        """Reset all counters."""
        self.total_seen = 0
        self.duplicates_dropped = 0


class RecordDeduplicator:
    """Yield unique records from an iterable by hashing a key subset.

    Args:
        key_fields: Fields used to determine uniqueness.  Pass ``None`` to
            hash the entire record.
        track_stats: Accumulate counts in ``self.stats`` (default ``False``).

    Example::

        dedup = RecordDeduplicator(key_fields=["review_id"])
        unique = list(dedup.deduplicate(records))
    """

    def __init__(
        self,
        key_fields: list[str] | None = None,
        track_stats: bool = False,
    ) -> None:
        self._key_fields = key_fields
        self._seen: set[str] = set()
        self.track_stats = track_stats
        self.stats = DeduplicationStats()

    def is_duplicate(self, record: dict[str, Any]) -> bool:
        """Return True if *record* has been seen before.

        As a side-effect, registers unseen records in the internal seen-set.

        Args:
            record: Record dict to test.

        Returns:
            True if the record is a duplicate, False otherwise.
        """
        digest = _record_hash(record, self._key_fields)
        if digest in self._seen:
            return True
        self._seen.add(digest)
        return False

    def deduplicate(self, records: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:
        """Yield records that have not been seen before.

        Args:
            records: Iterator of record dicts.

        Yields:
            Unique records in the order they first appear.
        """
        for record in records:
            if self.track_stats:
                self.stats.total_seen += 1
            if self.is_duplicate(record):
                if self.track_stats:
                    self.stats.duplicates_dropped += 1
                logger.debug("Duplicate dropped: %s", _record_hash(record, self._key_fields)[:8])
            else:
                yield record

    def reset(self) -> None:
        """Clear the internal seen-set and reset stats."""
        self._seen.clear()
        self.stats.reset()
        logger.info("RecordDeduplicator reset; seen-set cleared.")

    @property
    def seen_count(self) -> int:
        """Number of unique hashes stored in the seen-set."""
        return len(self._seen)
