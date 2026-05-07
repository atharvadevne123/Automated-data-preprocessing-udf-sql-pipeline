#!/usr/bin/env python3
"""Benchmark split_files.py performance on large synthetic datasets."""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)


def generate_synthetic_jsonl(path: Path, num_records: int) -> None:
    """Write *num_records* synthetic JSON records to *path*.

    Args:
        path: Output file path.
        num_records: Number of records to generate.
    """
    logger.info("Generating %d synthetic records -> %s", num_records, path)
    with open(path, "w", encoding="utf-8") as f:
        for i in range(num_records):
            record = {
                "id": i,
                "text": f"This is synthetic review number {i} for benchmarking purposes.",
                "stars": i % 5 + 1,
                "useful": i % 3,
                "funny": i % 2,
                "cool": i % 4,
            }
            f.write(json.dumps(record) + "\n")
    size_mb = path.stat().st_size / (1024 * 1024)
    logger.info("Generated %.2f MB", size_mb)


def benchmark_split(
    input_path: Path, num_chunks: int, output_dir: Path
) -> dict[str, float]:
    """Time a split operation and return metrics.

    Args:
        input_path: Path to input JSONL file.
        num_chunks: Number of output chunks.
        output_dir: Directory for output files.

    Returns:
        Dict with keys: elapsed_sec, records_per_sec, mb_per_sec.
    """
    from split_files import split_file  # local import to benchmark just split

    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = str(output_dir / "chunk_")
    file_size_mb = input_path.stat().st_size / (1024 * 1024)

    start = time.perf_counter()
    outputs = split_file(input_path, prefix, num_chunks)
    elapsed = time.perf_counter() - start

    total_lines = sum(
        sum(1 for _ in open(o, encoding="utf-8")) for o in outputs
    )
    records_per_sec = total_lines / elapsed if elapsed > 0 else float("inf")
    mb_per_sec = file_size_mb / elapsed if elapsed > 0 else float("inf")

    return {
        "elapsed_sec": round(elapsed, 4),
        "records_per_sec": round(records_per_sec, 1),
        "mb_per_sec": round(mb_per_sec, 3),
        "total_records": total_lines,
        "num_chunks": len(outputs),
    }


def main(argv: list[str] | None = None) -> None:
    """Run the benchmark with configurable parameters."""
    parser = argparse.ArgumentParser(description="Benchmark split_files.py performance.")
    parser.add_argument(
        "--records", type=int, default=10_000, help="Number of synthetic records (default: 10000)."
    )
    parser.add_argument(
        "--chunks", type=int, default=10, help="Number of output chunks (default: 10)."
    )
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        input_file = tmp / "synthetic.jsonl"
        output_dir = tmp / "chunks"

        generate_synthetic_jsonl(input_file, args.records)
        metrics = benchmark_split(input_file, args.chunks, output_dir)

    logger.info(
        "Benchmark results: %.4fs elapsed | %.0f records/s | %.3f MB/s | %d records in %d chunks",
        metrics["elapsed_sec"],
        metrics["records_per_sec"],
        metrics["mb_per_sec"],
        metrics["total_records"],
        metrics["num_chunks"],
    )


if __name__ == "__main__":
    main()
