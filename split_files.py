#!/usr/bin/env python3
"""Split large newline-delimited JSON files into smaller chunks."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

__version__ = "1.2.0"


def get_file_size_mb(filepath: Path) -> float:
    """Return the file size of *filepath* in megabytes.

    Args:
        filepath: Path to the file.

    Returns:
        File size in MB, or 0.0 if the file does not exist.
    """
    try:
        return filepath.stat().st_size / (1024 * 1024)
    except OSError:
        return 0.0


def count_lines(filepath: Path) -> int:
    """Count total lines in a file efficiently.

    Args:
        filepath: Path to the file to count.

    Returns:
        Total number of lines.

    Raises:
        OSError: if the file cannot be read.
    """
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
    """Split *input_file* into *num_files* chunks and return list of output paths.

    The last chunk absorbs any remainder lines so that no records are dropped.

    Args:
        input_file: Path to the source newline-delimited JSON file.
        output_prefix: String prefix for each output filename (e.g. ``"split_file_"``).
        num_files: Desired number of output files (>= 1).

    Returns:
        Ordered list of output file paths that were written.

    Raises:
        FileNotFoundError: if *input_file* does not exist.
        ValueError: if *num_files* < 1 or *input_file* is empty.
        OSError: on read/write failures.
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if num_files < 1:
        raise ValueError(f"num_files must be >= 1, got {num_files}")

    file_size_mb = get_file_size_mb(input_file)
    logger.info("Counting lines in %s (%.2f MB) ...", input_file, file_size_mb)
    total_lines = count_lines(input_file)
    if total_lines == 0:
        raise ValueError(f"Input file is empty: {input_file}")

    # Cap num_files so every output file gets at least one line
    if num_files > total_lines:
        logger.warning(
            "num_files (%d) exceeds total lines (%d); capping at %d",
            num_files,
            total_lines,
            total_lines,
        )
        num_files = total_lines

    lines_per_file = total_lines // num_files
    remainder = total_lines % num_files
    logger.info(
        "Total lines: %d, chunks: %d, ~%d lines/chunk",
        total_lines,
        num_files,
        lines_per_file,
    )

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
                    chunk_size_mb = get_file_size_mb(Path(output_filename))
                    logger.info(
                        "Written %s (%d lines, %.3f MB)",
                        output_filename,
                        written,
                        chunk_size_mb,
                    )
                except OSError as e:
                    logger.error("Failed to write %s: %s", output_filename, e)
                    raise
    except OSError as e:
        logger.error("Failed to open %s: %s", input_file, e)
        raise

    logger.info("Done — split into %d file(s).", len(output_files))
    return output_files


def split_file_stream(input_file: Path, num_files: int) -> list[list[str]]:
    """Split *input_file* into *num_files* in-memory chunk lists.

    Unlike :func:`split_file`, this function does not write to disk — it
    returns the lines grouped into chunks.  Useful for testing or when the
    caller controls output serialisation.

    Args:
        input_file: Path to the source JSONL file.
        num_files: Desired number of output chunks (>= 1).

    Returns:
        List of *num_files* lists, each containing the raw line strings for
        that chunk.  Lines include the trailing newline character.

    Raises:
        FileNotFoundError: if *input_file* does not exist.
        ValueError: if *num_files* < 1 or the file is empty.
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if num_files < 1:
        raise ValueError(f"num_files must be >= 1, got {num_files}")

    with open(input_file, encoding="utf-8") as f:
        all_lines = f.readlines()

    total = len(all_lines)
    if total == 0:
        raise ValueError(f"Input file is empty: {input_file}")

    if num_files > total:
        num_files = total

    lines_per_chunk = total // num_files
    remainder = total % num_files

    chunks: list[list[str]] = []
    start = 0
    for i in range(num_files):
        end = start + lines_per_chunk + (remainder if i == num_files - 1 else 0)
        chunks.append(all_lines[start:end])
        start = end
    return chunks


def estimate_chunks_for_size(input_file: Path, target_size_mb: float) -> int:
    """Estimate how many chunks to split *input_file* into to reach *target_size_mb* per chunk.

    Args:
        input_file: Path to the source file.
        target_size_mb: Desired maximum size per output chunk in MB.

    Returns:
        Recommended number of chunks (at least 1).

    Raises:
        FileNotFoundError: if *input_file* does not exist.
        ValueError: if *target_size_mb* <= 0.
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if target_size_mb <= 0:
        raise ValueError(f"target_size_mb must be > 0, got {target_size_mb}")

    file_size_mb = get_file_size_mb(input_file)
    if file_size_mb == 0.0:
        return 1
    chunks = max(1, int(file_size_mb / target_size_mb) + (1 if file_size_mb % target_size_mb else 0))
    return chunks


def main() -> None:
    """CLI entry point for splitting large JSONL files."""
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
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for output files; created if it does not exist (default: current directory)",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=10,
        help="Number of output files to split into (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview the split plan (line counts per chunk) without writing any files.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    args = parser.parse_args()

    if args.dry_run:
        try:
            total = count_lines(args.input_file)
            num = min(args.num_files, total) if total > 0 else args.num_files
            lpp = total // num if num > 0 else 0
            rem = total % num if num > 0 else 0
            logger.info("[DRY RUN] Input: %s, total lines: %d", args.input_file, total)
            for i in range(num):
                size = lpp + (rem if i == num - 1 else 0)
                logger.info("[DRY RUN]   chunk %d: %d lines", i + 1, size)
            logger.info("[DRY RUN] No files written.")
        except (FileNotFoundError, OSError) as e:
            logger.error("%s", e)
            sys.exit(1)
        return

    prefix = args.output_prefix
    if args.output_dir is not None:
        try:
            args.output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error("Cannot create output directory %s: %s", args.output_dir, e)
            sys.exit(1)
        prefix = str(args.output_dir / args.output_prefix)

    try:
        split_file(args.input_file, prefix, args.num_files)
    except (FileNotFoundError, ValueError, OSError) as e:
        logger.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
