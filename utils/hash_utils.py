"""Stable hashing utilities for records and text."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def sha256_hex(text: str) -> str:
    """Return the SHA-256 hex digest of *text* encoded as UTF-8.

    Args:
        text: Input string.

    Returns:
        64-character lowercase hex digest.
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def md5_hex(text: str) -> str:
    """Return the MD5 hex digest of *text* encoded as UTF-8.

    Args:
        text: Input string.

    Returns:
        32-character lowercase hex digest.

    Note:
        MD5 is not cryptographically secure.  Use only for checksums / IDs.
    """
    return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()  # noqa: S324


def record_fingerprint(
    record: dict[str, Any],
    key_fields: list[str] | None = None,
    algorithm: str = "sha256",
) -> str:
    """Compute a stable fingerprint for *record*.

    Args:
        record: Dict to fingerprint.
        key_fields: If supplied, only these fields are included in the hash.
            Fields are sorted alphabetically before hashing.
        algorithm: Hash algorithm — ``"sha256"`` (default) or ``"md5"``.

    Returns:
        Hex digest string.

    Raises:
        ValueError: If *algorithm* is not ``"sha256"`` or ``"md5"``.
    """
    if algorithm not in ("sha256", "md5"):
        raise ValueError(f"algorithm must be 'sha256' or 'md5', got {algorithm!r}")
    if key_fields is not None:
        payload = {k: record.get(k) for k in sorted(key_fields)}
    else:
        payload = dict(sorted(record.items()))
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    if algorithm == "sha256":
        return sha256_hex(raw)
    return md5_hex(raw)


def short_id(record: dict[str, Any], length: int = 8) -> str:
    """Return a short hex ID derived from *record*'s SHA-256 fingerprint.

    Args:
        record: Dict to fingerprint.
        length: Number of hex characters to return (1–64).

    Returns:
        First *length* characters of the SHA-256 fingerprint.

    Raises:
        ValueError: If *length* is not in [1, 64].
    """
    if not (1 <= length <= 64):
        raise ValueError(f"length must be between 1 and 64, got {length}")
    return record_fingerprint(record)[:length]


def content_hash_file(path: str) -> str:
    """Compute the SHA-256 hash of a file's contents.

    Args:
        path: File system path to hash.

    Returns:
        Lowercase hex digest.

    Raises:
        OSError: If the file cannot be read.
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
