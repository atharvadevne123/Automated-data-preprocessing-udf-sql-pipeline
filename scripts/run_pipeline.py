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


def run_pipeline(
    input_path: Path,
    output_dir: Path,
    output_format: str = "jsonl",
    sample_rate: float = 1.0,
    min_stars: float | None = None,
    max_records: int | None = None,
) -> dict:
    """Execute the full cleaning → sentiment → export pipeline.

    Args:
        input_path: Path to the source JSONL file.
        output_dir: Directory where output files are written.
        output_format: One of ``'jsonl'``, ``'json'``, ``'csv'``.
        sample_rate: Fraction of records to keep (0.0, 1.0].
        min_stars: If set, only keep records with stars >= this value.
        max_records: If set, stop after this many output records.

    Returns:
        Dict summary with keys: input, output, records_read, records_written, elapsed_sec.
    """
    import time

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
    cleaner = TextCleaner(track_stats=True)
    analyzer = SentimentAnalyzer()
    exporter = DataExporter()

    logger.info("Reading from %s ...", input_path)
    records = proc.process_file_to_list(input_path)
    records_read = len(records)
    logger.info("Read %d records after filter/sample", records_read)

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
    args = parser.parse_args()

    try:
        summary = run_pipeline(
            input_path=args.input,
            output_dir=args.output,
            output_format=args.format,
            sample_rate=args.sample_rate,
            min_stars=args.min_stars,
            max_records=args.max_records,
        )
        print(f"Done. {summary['records_written']} records → {summary['output']} ({summary['elapsed_sec']}s)")
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
