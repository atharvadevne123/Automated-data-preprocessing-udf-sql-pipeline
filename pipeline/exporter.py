"""Export processed pipeline records to CSV, JSON, and JSONL formats."""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class DataExporter:
    """Export lists of record dicts to various output formats.

    Supported formats: ``jsonl`` (newline-delimited JSON), ``json`` (array),
    ``csv`` (comma-separated values).

    Example::

        exporter = DataExporter()
        exporter.to_jsonl(records, Path("output/reviews.jsonl"))
        exporter.to_csv(records, Path("output/reviews.csv"), fields=["stars", "text"])
    """

    def __init__(self, encoding: str = "utf-8") -> None:
        self._encoding = encoding

    @staticmethod
    def _ensure_parent(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)

    def to_jsonl(self, records: list[dict[str, Any]], path: Path) -> int:
        """Write *records* as newline-delimited JSON to *path*.

        Args:
            records: List of dicts to serialise.
            path: Destination file path (parent dirs created automatically).

        Returns:
            Number of records written.

        Raises:
            OSError: on write failure.
        """
        self._ensure_parent(path)
        count = 0
        try:
            with open(path, "w", encoding=self._encoding) as fh:
                for record in records:
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
        except OSError as exc:
            logger.error("Failed to write JSONL to %s: %s", path, exc)
            raise
        logger.info("Exported %d records to JSONL: %s", count, path)
        return count

    def to_json(self, records: list[dict[str, Any]], path: Path, indent: int = 2) -> int:
        """Write *records* as a JSON array to *path*.

        Args:
            records: List of dicts to serialise.
            path: Destination file path.
            indent: JSON indentation level.

        Returns:
            Number of records written.

        Raises:
            OSError: on write failure.
        """
        self._ensure_parent(path)
        try:
            with open(path, "w", encoding=self._encoding) as fh:
                json.dump(records, fh, indent=indent, ensure_ascii=False)
        except OSError as exc:
            logger.error("Failed to write JSON to %s: %s", path, exc)
            raise
        logger.info("Exported %d records to JSON: %s", len(records), path)
        return len(records)

    def to_csv(
        self,
        records: list[dict[str, Any]],
        path: Path,
        fields: list[str] | None = None,
    ) -> int:
        """Write *records* as CSV to *path*.

        Args:
            records: List of dicts to export.
            path: Destination file path.
            fields: Column names; inferred from the first record if omitted.

        Returns:
            Number of records written (excluding header).

        Raises:
            ValueError: if *records* is empty and *fields* is not provided.
            OSError: on write failure.
        """
        if not records and fields is None:
            raise ValueError("Cannot infer CSV columns from an empty record list — pass fields=")
        self._ensure_parent(path)
        fieldnames = fields or list(records[0].keys())
        count = 0
        try:
            with open(path, "w", newline="", encoding=self._encoding) as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                for record in records:
                    writer.writerow(record)
                    count += 1
        except OSError as exc:
            logger.error("Failed to write CSV to %s: %s", path, exc)
            raise
        logger.info("Exported %d records to CSV: %s", count, path)
        return count

    def export(
        self,
        records: list[dict[str, Any]],
        path: Path,
        fmt: str = "jsonl",
        **kwargs: Any,
    ) -> int:
        """Dispatch to the appropriate export method based on *fmt*.

        Args:
            records: Records to export.
            path: Output path.
            fmt: One of ``'jsonl'``, ``'json'``, ``'csv'``.
            **kwargs: Extra keyword arguments forwarded to the format method.

        Returns:
            Number of records written.

        Raises:
            ValueError: for unsupported *fmt* values.
        """
        fmt = fmt.lower().lstrip(".")
        if fmt == "jsonl":
            return self.to_jsonl(records, path)
        if fmt == "json":
            return self.to_json(records, path, **kwargs)
        if fmt == "csv":
            return self.to_csv(records, path, **kwargs)
        raise ValueError(f"Unsupported export format: {fmt!r}. Use 'jsonl', 'json', or 'csv'.")
