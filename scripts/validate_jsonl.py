#!/usr/bin/env python3
"""CLI tool to validate a newline-delimited JSON (JSONL) file."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from utils.logger import get_logger
from utils.validators import ValidationError, validate_jsonl_file

logger = get_logger(__name__)


def validate_required_fields(path: Path, required_fields: list[str], max_errors: int = 10) -> list[str]:
    """Check that every record in *path* contains all *required_fields*.

    Args:
        path: Path to the JSONL file.
        required_fields: Field names that must exist in every record.
        max_errors: Stop after this many schema errors.

    Returns:
        List of error strings (empty if all records pass).
    """
    errors: list[str] = []
    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                errors.append(f"Line {lineno}: expected object, got {type(record).__name__}")
                if len(errors) >= max_errors:
                    break
                continue
            missing = [fld for fld in required_fields if fld not in record]
            if missing:
                errors.append(f"Line {lineno}: missing required fields {missing}")
                if len(errors) >= max_errors:
                    break
    return errors


def main(argv: list[str] | None = None) -> int:
    """Validate a JSONL file and report any parse or schema errors.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code: 0 = valid, 1 = invalid, 2 = usage error.
    """
    parser = argparse.ArgumentParser(description="Validate a newline-delimited JSON file for parse and schema errors.")
    parser.add_argument("file", type=Path, help="Path to the JSONL file to validate.")
    parser.add_argument(
        "--max-errors",
        type=int,
        default=10,
        help="Stop reporting after this many errors (default: 10).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress informational output; only show errors.",
    )
    parser.add_argument(
        "--required-fields",
        nargs="+",
        metavar="FIELD",
        default=[],
        help="One or more field names that must be present in every record.",
    )
    args = parser.parse_args(argv)

    try:
        errors = validate_jsonl_file(args.file, max_errors=args.max_errors)
    except ValidationError as exc:
        logger.error("%s", exc)
        return 2

    if not errors and args.required_fields:
        errors = validate_required_fields(args.file, args.required_fields, max_errors=args.max_errors)

    if errors:
        for err in errors:
            logger.error("%s", err)
        logger.error("Validation FAILED: %d error(s) in %s", len(errors), args.file)
        return 1

    if not args.quiet:
        logger.info("Validation PASSED: %s is a valid JSONL file.", args.file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
