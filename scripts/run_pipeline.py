"""Main pipeline orchestration CLI.

Chains cleaning, sentiment analysis, and export into a single command:

    python scripts/run_pipeline.py reviews.jsonl --output results/ --format jsonl
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _process_chunk(args: tuple) -> list[dict]:
    """Worker function: clean + enrich a chunk of records."""
    from pipeline.cleaner import TextCleaner
    from pipeline.sentiment import SentimentAnalyzer

    chunk, max_records, current_count = args
    cleaner = TextCleaner(track_stats=False)
    analyzer = SentimentAnalyzer()
    result = []
    for rec in chunk:
        if max_records is not None and current_count + len(result) >= max_records:
            break
        cleaner.clean_record(rec)
        analyzer.enrich_record(rec)
        result.append(rec)
    return result


def run_pipeline(
    input_path: Path,
    output_dir: Path,
    output_format: str = "jsonl",
    sample_rate: float = 1.0,
    min_stars: float | None = None,
    max_records: int | None = None,
    workers: int = 1,
) -> dict:
    """Execute the full cleaning → sentiment → export pipeline.

    Args:
        input_path: Path to the source JSONL file.
        output_dir: Directory where output files are written.
        output_format: One of ``'jsonl'``, ``'json'``, ``'csv'``.
        sample_rate: Fraction of records to keep (0.0, 1.0].
        min_stars: If set, only keep records with stars >= this value.
        max_records: If set, stop after this many output records.
        workers: Number of parallel worker processes for cleaning/enrichment (default 1).

    Returns:
        Dict summary with keys: input, output, records_read, records_written, elapsed_sec, workers.
    """
    import math
    import time
    from concurrent.futures import ProcessPoolExecutor

    from pipeline.cleaner import TextCleaner
    from pipeline.exporter import DataExporter
    from pipeline.processor import RecordProcessor
    from pipeline.sentiment import SentimentAnalyzer

    start = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    filters = []
    if min_stars is not None:
        filters.append(lambda r, s=min_stars: float(r.get("stars", 0)) >= s)

    proc = RecordProcessor(filters=filters, sample_rate=sample_rate)
    exporter = DataExporter()

    logger.info("Reading from %s ...", input_path)
    records = proc.process_file_to_list(input_path)
    records_read = len(records)
    logger.info("Read %d records after filter/sample (workers=%d)", records_read, workers)

    cleaner = TextCleaner(track_stats=True)
    if workers > 1 and records_read > 0:
        chunk_size = max(1, math.ceil(records_read / workers))
        chunks = [records[i : i + chunk_size] for i in range(0, records_read, chunk_size)]
        args_list = [(chunk, max_records, i * chunk_size) for i, chunk in enumerate(chunks)]
        with ProcessPoolExecutor(max_workers=workers) as pool:
            chunk_results = list(pool.map(_process_chunk, args_list))
        cleaned = [rec for chunk in chunk_results for rec in chunk]
        if max_records is not None:
            cleaned = cleaned[:max_records]
    else:
        analyzer = SentimentAnalyzer()
        cleaned = []
        for rec in records:
            cleaner.clean_record(rec)
            analyzer.enrich_record(rec)
            cleaned.append(rec)
            if max_records is not None and len(cleaned) >= max_records:
                break

    out_path = output_dir / f"pipeline_output.{output_format.lstrip('.')}"
    records_written = exporter.export(cleaned, out_path, fmt=output_format)
    elapsed = time.perf_counter() - start

    summary = {
        "input": str(input_path),
        "output": str(out_path),
        "records_read": records_read,
        "records_written": records_written,
        "elapsed_sec": round(elapsed, 3),
        "workers": workers,
        "cleaner_stats": cleaner.stats.to_dict(),
    }
    logger.info("Pipeline complete: %d records written to %s in %.2fs", records_written, out_path, elapsed)
    return summary


def main() -> None:
    """CLI entry point for run_pipeline."""
    parser = argparse.ArgumentParser(
        description="End-to-end Yelp pipeline: clean → sentiment → export.",
    )
    parser.add_argument("input", type=Path, help="Source JSONL file.")
    parser.add_argument("--output", type=Path, default=Path("output"), help="Output directory (default: output/).")
    parser.add_argument(
        "--format", choices=["jsonl", "json", "csv"], default="jsonl", help="Export format (default: jsonl)."
    )
    parser.add_argument("--sample-rate", type=float, default=1.0, help="Fraction of records to process (default: 1.0).")
    parser.add_argument("--min-stars", type=float, default=None, help="Minimum star rating filter.")
    parser.add_argument("--max-records", type=int, default=None, help="Stop after this many output records.")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel worker processes for cleaning/enrichment (default: 1).",
    )
    args = parser.parse_args()

    try:
        summary = run_pipeline(
            input_path=args.input,
            output_dir=args.output,
            output_format=args.format,
            sample_rate=args.sample_rate,
            min_stars=args.min_stars,
            max_records=args.max_records,
            workers=args.workers,
        )
        print(f"Done. {summary['records_written']} records → {summary['output']} ({summary['elapsed_sec']}s)")
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
