#!/usr/bin/env python3
"""CLI tool to run sentiment analysis on a JSONL file and export enriched records."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.sentiment import SentimentAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


def analyze_file(
    input_path: Path,
    output_path: Path,
    text_field: str = "text",
    positive_threshold: float = 0.1,
    negative_threshold: float = -0.1,
    output_format: str = "jsonl",
) -> dict[str, int]:
    """Read JSONL records, enrich with sentiment, and write to output.

    Args:
        input_path: Path to input JSONL file.
        output_path: Path to write enriched output.
        text_field: Record key containing review text.
        positive_threshold: Polarity above this = positive.
        negative_threshold: Polarity below this = negative.
        output_format: One of ``'jsonl'`` or ``'json'`` (default ``'jsonl'``).

    Returns:
        Dict with ``total``, ``positive``, ``negative``, ``neutral`` counts.

    Raises:
        FileNotFoundError: if input_path does not exist.
        ValueError: if output_format is not supported.
    """
    if output_format not in ("jsonl", "json"):
        raise ValueError(f"Unsupported output_format: {output_format!r}. Use 'jsonl' or 'json'.")
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    analyzer = SentimentAnalyzer(
        positive_threshold=positive_threshold,
        negative_threshold=negative_threshold,
    )
    counts: dict[str, int] = {"total": 0, "positive": 0, "negative": 0, "neutral": 0}

    records: list[dict] = []
    with open(input_path, encoding="utf-8") as fin:
        for lineno, raw in enumerate(fin, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.warning("Skipping invalid JSON at line %d: %s", lineno, exc)
                continue
            analyzer.enrich_record(record, text_field=text_field)
            records.append(record)
            counts["total"] += 1
            label = record.get("sentiment_label", "neutral")
            if label in counts:
                counts[label] += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fout:
        if output_format == "json":
            fout.write(json.dumps(records, ensure_ascii=False, indent=2))
        else:
            for record in records:
                fout.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info(
        "Sentiment analysis complete: %d total | %d positive | %d negative | %d neutral",
        counts["total"], counts["positive"], counts["negative"], counts["neutral"],
    )
    return counts


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for sentiment analysis pipeline.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code: 0 = success, 1 = error.
    """
    parser = argparse.ArgumentParser(
        description="Enrich a JSONL file with TextBlob sentiment scores."
    )
    parser.add_argument("input", type=Path, help="Path to input JSONL file.")
    parser.add_argument("output", type=Path, help="Path to write enriched JSONL output.")
    parser.add_argument(
        "--text-field", default="text", help="Record field containing review text (default: text)."
    )
    parser.add_argument(
        "--positive-threshold", type=float, default=0.1,
        help="Polarity threshold for positive label (default: 0.1).",
    )
    parser.add_argument(
        "--negative-threshold", type=float, default=-0.1,
        help="Polarity threshold for negative label (default: -0.1).",
    )
    parser.add_argument(
        "--output-format", default="jsonl", choices=["jsonl", "json"],
        help="Output format: 'jsonl' (default) or 'json' array.",
    )
    args = parser.parse_args(argv)

    try:
        counts = analyze_file(
            args.input,
            args.output,
            text_field=args.text_field,
            positive_threshold=args.positive_threshold,
            negative_threshold=args.negative_threshold,
            output_format=args.output_format,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        return 1

    print(json.dumps(counts, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
