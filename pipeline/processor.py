"""JSONL record processor: filter, transform, and sample pipeline records."""

from __future__ import annotations

import json
import logging
import random
from pathlib import Path
from typing import Any, Callable, Iterator

logger = logging.getLogger(__name__)


class RecordProcessor:
    """Filter, transform, and sample newline-delimited JSON records.

    Args:
        filters: Optional list of callables; a record is kept only when ALL return True.
        transforms: Optional list of callables applied sequentially to each record.
        sample_rate: Float in (0, 1] — fraction of records to keep after filtering.
        seed: Random seed for reproducible sampling.

    Example::

        proc = RecordProcessor(
            filters=[lambda r: r.get("stars", 0) >= 4],
            sample_rate=0.1,
        )
        for record in proc.process_file(Path("reviews.jsonl")):
            print(record)
    """

    def __init__(
        self,
        filters: list[Callable[[dict[str, Any]], bool]] | None = None,
        transforms: list[Callable[[dict[str, Any]], dict[str, Any]]] | None = None,
        sample_rate: float = 1.0,
        seed: int | None = None,
    ) -> None:
        if not (0.0 < sample_rate <= 1.0):
            raise ValueError(f"sample_rate must be in (0, 1], got {sample_rate}")
        self._filters = filters or []
        self._transforms = transforms or []
        self._sample_rate = sample_rate
        self._rng = random.Random(seed)

    def _passes_filters(self, record: dict[str, Any]) -> bool:
        """Return True if all filters accept the record."""
        return all(f(record) for f in self._filters)

    def _apply_transforms(self, record: dict[str, Any]) -> dict[str, Any]:
        """Apply each transform in sequence and return the result."""
        for t in self._transforms:
            record = t(record)
        return record

    def process_record(self, record: dict[str, Any]) -> dict[str, Any] | None:
        """Process a single record through filters, sampling, and transforms.

        Args:
            record: Raw dict to process.

        Returns:
            Transformed dict if the record passes all filters and sampling, else None.
        """
        if not self._passes_filters(record):
            return None
        if self._sample_rate < 1.0 and self._rng.random() >= self._sample_rate:
            return None
        return self._apply_transforms(record)

    def process_records(self, records: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:
        """Yield processed records from an iterator.

        Args:
            records: Iterator of raw dicts.

        Yields:
            Processed records that pass filters and sampling.
        """
        kept = 0
        dropped = 0
        for record in records:
            result = self.process_record(record)
            if result is not None:
                kept += 1
                yield result
            else:
                dropped += 1
        logger.info("RecordProcessor: kept=%d dropped=%d", kept, dropped)

    def process_file(self, path: Path) -> Iterator[dict[str, Any]]:
        """Read a JSONL file and yield processed records.

        Args:
            path: Path to newline-delimited JSON file.

        Yields:
            Processed records.

        Raises:
            FileNotFoundError: if *path* does not exist.
            OSError: on read failure.
        """
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        logger.info("Processing file: %s", path)

        def _lines() -> Iterator[dict[str, Any]]:
            with open(path, encoding="utf-8") as fh:
                for lineno, raw in enumerate(fh, start=1):
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        yield json.loads(raw)
                    except json.JSONDecodeError as exc:
                        logger.warning("Skipping invalid JSON at line %d: %s", lineno, exc)

        yield from self.process_records(_lines())

    def process_file_to_list(self, path: Path) -> list[dict[str, Any]]:
        """Convenience wrapper that materialises process_file() into a list."""
        return list(self.process_file(path))
