"""CLI script: partition a JSONL file by a field value."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pipeline.partitioner import FilePartitioner
from utils.file_utils import iter_jsonl

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
        description="Partition a JSONL file into separate files by a field value."
    )
    parser.add_argument("input", type=Path, help="Input JSONL file path.")
    parser.add_argument("output_dir", type=Path, help="Directory for output partition files.")
    parser.add_argument(
        "--field",
        default="stars",
        help="Record field to partition by (default: stars).",
    )
    parser.add_argument(
        "--prefix",
        default="partition",
        help="Output filename prefix (default: partition).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the partition_data CLI.

    Args:
        argv: Argument list.

    Returns:
        Exit code (0 on success, 1 on error).
    """
    args = parse_args(argv)

    if not args.input.exists():
        logger.error("Input file not found: %s", args.input)
        return 1

    field = args.field

    partitioner = FilePartitioner(
        key_func=lambda r: r.get(field),
        output_dir=args.output_dir,
        prefix=args.prefix,
    )

    try:
        records = list(iter_jsonl(args.input))
        partitioner.add_batch(records)
        paths = partitioner.flush()
    except OSError as exc:
        logger.error("I/O error: %s", exc)
        return 1

    sizes = partitioner.partition_sizes()
    logger.info("Partitioned %d records into %d files:", len(records), len(paths))
    for key, count in sorted(sizes.items(), key=lambda x: str(x[0])):
        logger.info("  %s = %s: %d records", field, key, count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
