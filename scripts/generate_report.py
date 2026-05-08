#!/usr/bin/env python3
"""Generate a processing summary report from aggregated pipeline statistics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.aggregator import StatsAggregator
from utils.logger import get_logger

logger = get_logger(__name__)


def aggregate_jsonl(input_path: Path) -> StatsAggregator:
    """Read a JSONL file and aggregate statistics.

    Args:
        input_path: Path to a newline-delimited JSON file (may include sentiment fields).

    Returns:
        Populated :class:`StatsAggregator`.

    Raises:
        FileNotFoundError: if input_path does not exist.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")

    agg = StatsAggregator()
    skipped = 0

    with open(input_path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw)
                agg.add(record)
            except json.JSONDecodeError as exc:
                logger.warning("Skipping invalid JSON at line %d: %s", lineno, exc)
                skipped += 1

    logger.info("Aggregated %d records (%d skipped)", agg.global_stats().total_records, skipped)
    return agg


def build_report(agg: StatsAggregator, top_n: int = 10) -> dict:
    """Build a JSON-serialisable report dict from aggregated stats.

    Args:
        agg: Populated StatsAggregator instance.
        top_n: Number of top businesses to include.

    Returns:
        Report dict with ``summary``, ``top_businesses``, and ``star_distribution`` keys.
    """
    global_stats = agg.global_stats().to_dict()
    top_biz = [b.to_dict() for b in agg.top_businesses(n=top_n)]
    all_biz = agg.all_business_stats()
    avg_review_count = (
        sum(b["review_count"] for b in all_biz) / len(all_biz)
        if all_biz
        else 0.0
    )
    return {
        "summary": {
            "total_records": global_stats["total_records"],
            "unique_businesses": len(all_biz),
            "average_reviews_per_business": round(avg_review_count, 2),
            "sentiment_counts": global_stats["sentiment_counts"],
        },
        "star_distribution": global_stats["star_distribution"],
        "top_businesses": top_biz,
    }


def main(argv: list[str] | None = None) -> int:
    """Generate a processing report from a JSONL file.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code: 0 = success, 1 = error.
    """
    parser = argparse.ArgumentParser(
        description="Generate a JSON summary report from a processed JSONL file."
    )
    parser.add_argument("input", type=Path, help="Path to input JSONL file.")
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Path to write JSON report (defaults to stdout).",
    )
    parser.add_argument(
        "--top-n", type=int, default=10,
        help="Number of top businesses to include in the report (default: 10).",
    )
    args = parser.parse_args(argv)

    try:
        agg = aggregate_jsonl(args.input)
        report = build_report(agg, top_n=args.top_n)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        return 1

    report_json = json.dumps(report, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report_json, encoding="utf-8")
        logger.info("Report written to %s", args.output)
    else:
        print(report_json)

    return 0


if __name__ == "__main__":
    sys.exit(main())
