#!/usr/bin/env python3
"""Split large newline-delimited JSON files into smaller chunks."""

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def count_lines(filepath: Path) -> int:
    """Count total lines in a file efficiently."""
    count = 0
    try:
        with open(filepath, encoding="utf-8") as f:
            for _ in f:
                count += 1
    except OSError as e:
        logger.error("Failed to read %s: %s", filepath, e)
        raise
    return count


def split_file(input_file: Path, output_prefix: str, num_files: int) -> list[str]:
    """Split input_file into num_files chunks; return list of output paths."""
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if num_files < 1:
        raise ValueError(f"num_files must be >= 1, got {num_files}")

    logger.info("Counting lines in %s ...", input_file)
    total_lines = count_lines(input_file)
    if total_lines == 0:
        raise ValueError(f"Input file is empty: {input_file}")

    lines_per_file = total_lines // num_files
    remainder = total_lines % num_files
    logger.info("Total lines: %d, ~%d lines per file", total_lines, lines_per_file)

    output_files: list[str] = []
    try:
        with open(input_file, encoding="utf-8") as f:
            for i in range(num_files):
                output_filename = f"{output_prefix}{i + 1}.json"
                # Last chunk absorbs any remainder so no lines are dropped
                chunk_size = lines_per_file + (remainder if i == num_files - 1 else 0)
                try:
                    with open(output_filename, "w", encoding="utf-8") as out:
                        written = 0
                        for _ in range(chunk_size):
                            line = f.readline()
                            if not line:
                                break
                            out.write(line)
                            written += 1
                    output_files.append(output_filename)
                    logger.info("Written %s (%d lines)", output_filename, written)
                except OSError as e:
                    logger.error("Failed to write %s: %s", output_filename, e)
                    raise
    except OSError as e:
        logger.error("Failed to open %s: %s", input_file, e)
        raise

    logger.info("Done — split into %d file(s).", len(output_files))
    return output_files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Split a large newline-delimited JSON file into smaller chunks."
    )
    parser.add_argument(
        "input_file",
        type=Path,
        nargs="?",
        default=Path("yelp_academic_dataset_review.json"),
        help="Path to the large input JSON file (default: yelp_academic_dataset_review.json)",
    )
    parser.add_argument(
        "--output-prefix",
        default="split_file_",
        help="Prefix for output filenames (default: split_file_)",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=10,
        help="Number of output files to split into (default: 10)",
    )
    args = parser.parse_args()

    try:
        split_file(args.input_file, args.output_prefix, args.num_files)
    except (FileNotFoundError, ValueError, OSError) as e:
        logger.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
