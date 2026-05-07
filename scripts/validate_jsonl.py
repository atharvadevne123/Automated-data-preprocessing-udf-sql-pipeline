#!/usr/bin/env python3
"""CLI tool to validate a newline-delimited JSON (JSONL) file."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from utils.logger import get_logger
from utils.validators import ValidationError, validate_jsonl_file

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    """Validate a JSONL file and report any parse errors.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code: 0 = valid, 1 = invalid, 2 = usage error.
    """
    parser = argparse.ArgumentParser(
        description="Validate a newline-delimited JSON file for parse errors."
    )
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
    args = parser.parse_args(argv)

    try:
        errors = validate_jsonl_file(args.file, max_errors=args.max_errors)
    except ValidationError as exc:
        logger.error("%s", exc)
        return 2

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
