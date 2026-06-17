"""File system utility helpers for pipeline I/O operations."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)


def ensure_dir(path: Path | str) -> Path:
    """Create *path* (and any missing parents) if it does not exist.

    Args:
        path: Directory path to create.

    Returns:
        The resolved :class:`~pathlib.Path` object.

    Raises:
        OSError: If the directory cannot be created.
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def file_line_count(path: Path | str) -> int:
    """Count newline characters in a file efficiently using buffered reads.

    Args:
        path: Path to the file.

    Returns:
        Number of newline-terminated lines.

    Raises:
        FileNotFoundError: If *path* does not exist.
        OSError: On read failure.
    """
    count = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            count += chunk.count(b"\n")
    return count


def iter_jsonl(path: Path | str, skip_errors: bool = True) -> Iterator[dict[str, Any]]:
    """Yield parsed dicts from a JSONL file.

    Args:
        path: Path to a newline-delimited JSON file.
        skip_errors: If True, skip malformed lines with a warning instead of
            raising (default True).

    Yields:
        Parsed record dicts.

    Raises:
        FileNotFoundError: If *path* does not exist.
        json.JSONDecodeError: If *skip_errors* is False and a line is invalid.
    """
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError as exc:
                if skip_errors:
                    logger.warning("Skipping invalid JSON at line %d: %s", lineno, exc)
                else:
                    raise


def write_jsonl(records: list[dict[str, Any]], path: Path | str) -> int:
    """Write *records* to a JSONL file.

    Args:
        records: Dicts to serialise as newline-delimited JSON.
        path: Output file path.  Parent directories are created automatically.

    Returns:
        Number of records written.

    Raises:
        OSError: On write failure.
    """
    p = Path(path)
    ensure_dir(p.parent)
    count = 0
    try:
        with open(p, "w", encoding="utf-8") as fh:
            for record in records:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
    except OSError as exc:
        logger.error("write_jsonl failed for %s: %s", path, exc)
        raise
    logger.info("Wrote %d records to %s.", count, path)
    return count


def file_size_mb(path: Path | str) -> float:
    """Return the size of *path* in megabytes.

    Args:
        path: File system path.

    Returns:
        Size in MB, or 0.0 if the file does not exist.
    """
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except OSError:
        return 0.0


def list_jsonl_files(directory: Path | str, recursive: bool = False) -> list[Path]:
    """Return all ``*.jsonl`` files in *directory*.

    Args:
        directory: Directory to search.
        recursive: If True, search subdirectories too (default False).

    Returns:
        Sorted list of :class:`~pathlib.Path` objects.
    """
    d = Path(directory)
    pattern = "**/*.jsonl" if recursive else "*.jsonl"
    return sorted(d.glob(pattern))


def safe_read_json(path: Path | str, default: Any = None) -> Any:
    """Read and parse a JSON file, returning *default* on any error.

    Args:
        path: Path to JSON file.
        default: Value returned if the file is missing or invalid (default None).

    Returns:
        Parsed JSON object, or *default*.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("safe_read_json(%s) failed: %s", path, exc)
        return default
