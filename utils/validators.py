"""Input validation helpers for the data pipeline."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class ValidationError(ValueError):
    """Raised when pipeline input fails validation."""


def validate_input_path(path: Path) -> Path:
    """Validate that *path* exists, is a file, and is readable.

    Args:
        path: Filesystem path to validate.

    Returns:
        The resolved absolute path.

    Raises:
        ValidationError: if the path does not exist, is a directory, or is not readable.
    """
    resolved = path.resolve()
    if not resolved.exists():
        raise ValidationError(f"Input path does not exist: {resolved}")
    if not resolved.is_file():
        raise ValidationError(f"Input path is not a regular file: {resolved}")
    if not os.access(resolved, os.R_OK):
        raise ValidationError(f"Input path is not readable: {resolved}")
    return resolved


def validate_num_files(n: int) -> int:
    """Validate that *n* is a positive integer.

    Args:
        n: Number of output files requested.

    Returns:
        *n* unchanged.

    Raises:
        ValidationError: if *n* is less than 1.
    """
    if n < 1:
        raise ValidationError(f"num_files must be >= 1, got {n}")
    return n


def validate_jsonl_file(path: Path, max_errors: int = 10) -> list[str]:
    """Scan *path* for JSON parse errors.

    Args:
        path: Path to a newline-delimited JSON file.
        max_errors: Stop after this many errors are found.

    Returns:
        List of error strings (empty if the file is valid).

    Raises:
        ValidationError: if *path* does not exist or cannot be read.
    """
    if not path.exists():
        raise ValidationError(f"File not found: {path}")
    errors: list[str] = []
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"Line {lineno}: {exc}")
                if len(errors) >= max_errors:
                    break
    return errors


def validate_output_prefix(prefix: str) -> str:
    """Validate that *prefix* is a non-empty string.

    Args:
        prefix: Filename prefix string.

    Returns:
        *prefix* unchanged.

    Raises:
        ValidationError: if the prefix is empty.
    """
    if not prefix:
        raise ValidationError("output_prefix must not be empty")
    return prefix


def coerce_record(record: Any, required_fields: list[str]) -> dict[str, Any]:
    """Ensure *record* is a dict containing all *required_fields*.

    Args:
        record: Parsed JSON value.
        required_fields: Field names that must be present.

    Returns:
        *record* cast to dict.

    Raises:
        ValidationError: if *record* is not a dict or is missing required fields.
    """
    if not isinstance(record, dict):
        raise ValidationError(f"Expected dict, got {type(record).__name__}")
    missing = [f for f in required_fields if f not in record]
    if missing:
        raise ValidationError(f"Record missing required fields: {missing}")
    return record
