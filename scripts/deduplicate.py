"""CLI script: deduplicate records in a JSONL file."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pipeline.deduplicator import RecordDeduplicator
from utils.file_utils import iter_jsonl, write_jsonl

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments.

    Args:
        argv: Argument list (defaults to sys.argv).

    Returns:
        Parsed namespace.
    """
    parser = argparse.ArgumentParser(
        description="Deduplicate records in a JSONL file using hash-based comparison."
    )
    parser.add_argument("input", type=Path, help="Input JSONL file path.")
    parser.add_argument("output", type=Path, help="Output JSONL file path.")
    parser.add_argument(
        "--key-fields",
        nargs="+",
        metavar="FIELD",
        help="Fields used to determine uniqueness. Default: full record hash.",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print deduplication statistics to stderr.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the deduplicate CLI.

    Args:
        argv: Argument list.

    Returns:
        Exit code (0 on success, 1 on error).
    """
    args = parse_args(argv)

    if not args.input.exists():
        logger.error("Input file not found: %s", args.input)
        return 1

    dedup = RecordDeduplicator(key_fields=args.key_fields, track_stats=True)

    try:
        records = list(iter_jsonl(args.input))
        unique = list(dedup.deduplicate(iter(records)))
        write_jsonl(unique, args.output)
    except OSError as exc:
        logger.error("I/O error: %s", exc)
        return 1

    if args.stats:
        s = dedup.stats
        print(
            f"Total: {s.total_seen}  Duplicates: {s.duplicates_dropped}"
            f"  Unique: {s.unique_count}",
            file=sys.stderr,
        )

    logger.info(
        "Deduplication complete: %d -> %d records written to %s",
        len(records),
        len(unique),
        args.output,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
