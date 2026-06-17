"""Partition pipeline records by field value into separate buckets."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


class RecordPartitioner:
    """Partition a list of records into named buckets by a key function.

    Args:
        key_func: Callable that accepts a record dict and returns a hashable
            partition key.  Common usage: ``lambda r: r.get("stars")``.
        default_key: Bucket label used when *key_func* returns ``None``
            (default ``"__other__"``).

    Example::

        p = RecordPartitioner(key_func=lambda r: r.get("stars"))
        p.add_batch(records)
        positive = p.get_partition(5.0)
    """

    def __init__(
        self,
        key_func: Callable[[dict[str, Any]], Any],
        default_key: str = "__other__",
    ) -> None:
        self._key_func = key_func
        self._default_key = default_key
        self._partitions: dict[Any, list[dict[str, Any]]] = {}

    def add(self, record: dict[str, Any]) -> None:
        """Add a single *record* to its partition.

        Args:
            record: Record dict to partition.
        """
        key = self._key_func(record)
        if key is None:
            key = self._default_key
        self._partitions.setdefault(key, []).append(record)

    def add_batch(self, records: list[dict[str, Any]]) -> None:
        """Add all records in *records* to their respective partitions.

        Args:
            records: List of record dicts.
        """
        for record in records:
            self.add(record)
        logger.info(
            "RecordPartitioner: %d records distributed into %d partition(s).",
            len(records),
            len(self._partitions),
        )

    def get_partition(self, key: Any) -> list[dict[str, Any]]:
        """Return records for a specific partition key.

        Args:
            key: The partition key returned by *key_func*.

        Returns:
            List of matching records, or empty list if the partition is absent.
        """
        return list(self._partitions.get(key, []))

    def partition_keys(self) -> list[Any]:
        """Return all known partition keys."""
        return list(self._partitions.keys())

    def partition_sizes(self) -> dict[Any, int]:
        """Return a mapping of partition key → record count."""
        return {k: len(v) for k, v in self._partitions.items()}

    def all_partitions(self) -> dict[Any, list[dict[str, Any]]]:
        """Return a shallow copy of all partition data.

        Returns:
            Dict mapping each partition key to its record list.
        """
        return {k: list(v) for k, v in self._partitions.items()}

    def reset(self) -> None:
        """Clear all partitions."""
        self._partitions.clear()
        logger.info("RecordPartitioner reset.")


class FilePartitioner(RecordPartitioner):
    """Extend :class:`RecordPartitioner` to write each partition to a JSONL file.

    Args:
        key_func: As in :class:`RecordPartitioner`.
        output_dir: Directory where partition files are written.
        prefix: File name prefix (e.g. ``"stars"`` → ``stars_5.jsonl``).
        default_key: As in :class:`RecordPartitioner`.
    """

    def __init__(
        self,
        key_func: Callable[[dict[str, Any]], Any],
        output_dir: Path,
        prefix: str = "partition",
        default_key: str = "__other__",
    ) -> None:
        super().__init__(key_func=key_func, default_key=default_key)
        self._output_dir = Path(output_dir)
        self._prefix = prefix

    def flush(self) -> list[Path]:
        """Write each partition to a JSONL file and return the paths.

        Returns:
            List of paths written.

        Raises:
            OSError: If a file cannot be created.
        """
        import json

        self._output_dir.mkdir(parents=True, exist_ok=True)
        paths: list[Path] = []
        for key, records in self._partitions.items():
            safe_key = str(key).replace("/", "_").replace("\\", "_")
            path = self._output_dir / f"{self._prefix}_{safe_key}.jsonl"
            try:
                with open(path, "w", encoding="utf-8") as fh:
                    for rec in records:
                        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                logger.info("Wrote %d records to %s", len(records), path)
                paths.append(path)
            except OSError as exc:
                logger.error("Failed to write partition %s: %s", key, exc)
                raise
        return paths
