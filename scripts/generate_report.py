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


_ALL_SECTIONS = {"summary", "star_distribution", "top_businesses"}


def build_report(agg: StatsAggregator, top_n: int = 10, sections: set[str] | None = None) -> dict:
    """Build a JSON-serialisable report dict from aggregated stats.

    Args:
        agg: Populated StatsAggregator instance.
        top_n: Number of top businesses to include.
        sections: Set of section names to include; all sections included if None.
            Valid values: ``'summary'``, ``'star_distribution'``, ``'top_businesses'``.

    Returns:
        Report dict with requested section keys.
    """
    include = sections if sections is not None else _ALL_SECTIONS
    global_stats = agg.global_stats().to_dict()
    all_biz = agg.all_business_stats()
    report: dict = {}
    if "summary" in include:
        avg_review_count = sum(b["review_count"] for b in all_biz) / len(all_biz) if all_biz else 0.0
        report["summary"] = {
            "total_records": global_stats["total_records"],
            "unique_businesses": len(all_biz),
            "average_reviews_per_business": round(avg_review_count, 2),
            "sentiment_counts": global_stats["sentiment_counts"],
        }
    if "star_distribution" in include:
        report["star_distribution"] = global_stats["star_distribution"]
    if "top_businesses" in include:
        report["top_businesses"] = [b.to_dict() for b in agg.top_businesses(n=top_n)]
    return report


def build_markdown_report(report: dict) -> str:
    """Render *report* as a Markdown string.

    Args:
        report: Dict produced by :func:`build_report`.

    Returns:
        Markdown-formatted report string.
    """
    lines: list[str] = ["# Pipeline Report", ""]
    if "summary" in report:
        summary = report["summary"]
        lines += [
            "## Summary",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Total records | {summary['total_records']} |",
            f"| Unique businesses | {summary['unique_businesses']} |",
            f"| Avg reviews/business | {summary['average_reviews_per_business']} |",
            "",
            "### Sentiment Counts",
            "",
            "| Sentiment | Count |",
            "|-----------|-------|",
        ]
        for sentiment, count in summary.get("sentiment_counts", {}).items():
            lines.append(f"| {sentiment} | {count} |")
        lines.append("")

    if "star_distribution" in report:
        lines += [
            "## Star Distribution",
            "",
            "| Stars | Count |",
            "|-------|-------|",
        ]
        for star, count in sorted(report.get("star_distribution", {}).items()):
            lines.append(f"| {star} | {count} |")
        lines.append("")

    if "top_businesses" in report:
        lines += ["## Top Businesses", ""]
        for biz in report.get("top_businesses", []):
            lines.append(
                f"- **{biz.get('business_id', 'unknown')}**: "
                f"{biz.get('review_count', 0)} reviews, "
                f"avg {biz.get('avg_stars', 0):.2f} stars"
            )

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Generate a processing report from a JSONL file.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code: 0 = success, 1 = error.
    """
    parser = argparse.ArgumentParser(description="Generate a summary report from a processed JSONL file.")
    parser.add_argument("input", type=Path, help="Path to input JSONL file.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to write report (defaults to stdout).",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top businesses to include in the report (default: 10).",
    )
    parser.add_argument(
        "--format",
        default="json",
        choices=["json", "markdown"],
        help="Output format: 'json' (default) or 'markdown'.",
    )
    parser.add_argument(
        "--sections",
        nargs="+",
        choices=sorted(_ALL_SECTIONS),
        default=None,
        metavar="SECTION",
        help="Sections to include (default: all). Choices: summary, star_distribution, top_businesses.",
    )
    args = parser.parse_args(argv)

    sections = set(args.sections) if args.sections else None
    try:
        agg = aggregate_jsonl(args.input)
        report = build_report(agg, top_n=args.top_n, sections=sections)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        return 1

    if args.format == "markdown":
        output_text = build_markdown_report(report)
    else:
        output_text = json.dumps(report, indent=2)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output_text, encoding="utf-8")
        logger.info("Report written to %s", args.output)
    else:
        print(output_text)

    return 0


if __name__ == "__main__":
    sys.exit(main())
