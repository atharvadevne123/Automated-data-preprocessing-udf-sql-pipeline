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


def validate_star_rating(stars: Any) -> float:
    """Validate that *stars* is a numeric value in the range [1.0, 5.0].

    Args:
        stars: Star rating value (int or float).

    Returns:
        *stars* cast to float.

    Raises:
        ValidationError: if *stars* is non-numeric or outside [1.0, 5.0].
    """
    try:
        value = float(stars)
    except (TypeError, ValueError):
        raise ValidationError(f"Star rating must be numeric, got: {stars!r}")
    if not (1.0 <= value <= 5.0):
        raise ValidationError(f"Star rating must be in [1.0, 5.0], got {value}")
    return value


def validate_email(email: str) -> str:
    """Validate that *email* looks like a valid email address.

    Performs a simple structural check (user@domain.tld) without DNS lookup.

    Args:
        email: Email address string to validate.

    Returns:
        *email* stripped of surrounding whitespace.

    Raises:
        ValidationError: if *email* does not match the expected pattern.
    """
    import re
    _EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    cleaned = (email or "").strip()
    if not cleaned or not _EMAIL_RE.match(cleaned):
        raise ValidationError(f"Invalid email address: {email!r}")
    return cleaned


def sanitize_text(text: str, max_length: int = 10_000) -> str:
    """Strip and truncate *text* to at most *max_length* characters.

    Also removes ASCII null bytes (``\\x00``) which can cause issues in SQL
    INSERT statements.

    Args:
        text: Input text to sanitise.
        max_length: Maximum allowed character length (default 10 000).

    Returns:
        Sanitised string.

    Raises:
        ValidationError: if *text* is not a string.
    """
    if not isinstance(text, str):
        raise ValidationError(f"Expected str for text, got {type(text).__name__}")
    cleaned = text.replace("\x00", "").strip()
    return cleaned[:max_length]


def validate_date_format(date_str: str) -> str:
    """Validate that *date_str* matches YYYY-MM-DD format.

    Args:
        date_str: Date string to validate.

    Returns:
        *date_str* unchanged.

    Raises:
        ValidationError: if the string does not match YYYY-MM-DD.
    """
    import re
    _DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if not isinstance(date_str, str) or not _DATE_RE.match(date_str):
        raise ValidationError(f"Date must be YYYY-MM-DD, got: {date_str!r}")
    return date_str


def validate_business_id_format(business_id: str) -> str:
    """Validate that *business_id* is a non-empty string (Yelp IDs are 22 chars).

    Args:
        business_id: Business identifier string.

    Returns:
        *business_id* unchanged.

    Raises:
        ValidationError: if the identifier is empty or not a string.
    """
    if not isinstance(business_id, str) or not business_id.strip():
        raise ValidationError(f"business_id must be a non-empty string, got: {business_id!r}")
    return business_id


def validate_coordinates(latitude: float, longitude: float) -> tuple[float, float]:
    """Validate geographic coordinates are within valid ranges.

    Args:
        latitude: Latitude in degrees; must be in [-90.0, 90.0].
        longitude: Longitude in degrees; must be in [-180.0, 180.0].

    Returns:
        Tuple of (latitude, longitude) as floats.

    Raises:
        ValidationError: if either value is out of range.
    """
    try:
        lat = float(latitude)
        lon = float(longitude)
    except (TypeError, ValueError):
        raise ValidationError(f"Coordinates must be numeric, got: lat={latitude!r}, lon={longitude!r}")
    if not (-90.0 <= lat <= 90.0):
        raise ValidationError(f"Latitude must be in [-90, 90], got {lat}")
    if not (-180.0 <= lon <= 180.0):
        raise ValidationError(f"Longitude must be in [-180, 180], got {lon}")
    return lat, lon


def validate_yelp_review_record(record: dict[str, Any]) -> dict[str, Any]:
    """Validate the required fields of a Yelp review record dict.

    Checks that ``review_id``, ``user_id``, ``business_id``, and ``stars``
    are present and that ``stars`` is valid.

    Args:
        record: Parsed JSON review record.

    Returns:
        The validated record unchanged.

    Raises:
        ValidationError: if any required field is missing or invalid.
    """
    coerce_record(record, ["review_id", "user_id", "business_id", "stars"])
    validate_star_rating(record["stars"])
    return record
