"""End-to-end integration tests for the full pipeline flow."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.aggregator import StatsAggregator
from pipeline.cleaner import TextCleaner
from pipeline.exporter import DataExporter
from pipeline.processor import RecordProcessor
from pipeline.sentiment import SentimentAnalyzer


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


@pytest.fixture
def review_jsonl(tmp_path):
    path = tmp_path / "reviews.jsonl"
    records = [
        {"review_id": f"r{i}", "business_id": f"b{i % 3}", "stars": float(i % 5 + 1),
         "text": f"<b>Review {i}</b> http://yelp.com" + ("!" * 5), "useful": i % 3}
        for i in range(30)
    ]
    _write_jsonl(path, records)
    return path


class TestFullPipeline:
    def test_process_clean_enrich_export(self, review_jsonl, tmp_path):
        """Filter → clean → enrich with sentiment → export to JSONL."""
        processor = RecordProcessor(
            filters=[lambda r: r.get("stars", 0) >= 3],
            seed=42,
        )
        cleaner = TextCleaner(lowercase=True, strip_urls=True, strip_html=True)
        analyzer = SentimentAnalyzer()
        exporter = DataExporter()

        raw_records = list(processor.process_file(review_jsonl))
        assert len(raw_records) > 0

        cleaned = [cleaner.clean_record(r, field="text") for r in raw_records]
        enriched = [analyzer.enrich_record(r) for r in cleaned]

        for r in enriched:
            assert "http" not in r.get("text", "")
            assert "<b>" not in r.get("text", "")
            assert "sentiment_label" in r

        output_path = tmp_path / "output.jsonl"
        count = exporter.to_jsonl(enriched, output_path)
        assert count == len(enriched)
        assert output_path.exists()

    def test_aggregate_after_processing(self, review_jsonl, tmp_path):
        """Run full pipeline and verify aggregated stats."""
        processor = RecordProcessor(filters=[lambda r: r.get("stars", 0) >= 1])
        analyzer = SentimentAnalyzer()
        agg = StatsAggregator()

        records = list(processor.process_file(review_jsonl))
        for r in records:
            analyzer.enrich_record(r)
            agg.add(r)

        stats = agg.global_stats()
        assert stats.total_records == 30
        assert sum(stats.star_distribution.values()) == 30

    def test_csv_export_after_processing(self, review_jsonl, tmp_path):
        """Process and export to CSV with selected fields."""
        processor = RecordProcessor(filters=[lambda r: r.get("stars", 0) >= 4])
        exporter = DataExporter()

        records = list(processor.process_file(review_jsonl))
        output = tmp_path / "output.csv"
        count = exporter.to_csv(records, output, fields=["review_id", "stars", "text"])
        assert output.exists()
        assert count == len(records)

    def test_pipeline_with_sample_rate(self, review_jsonl):
        """Sampling should reduce output without error."""
        processor = RecordProcessor(sample_rate=0.5, seed=0)
        records = list(processor.process_file(review_jsonl))
        assert len(records) < 30

    def test_export_json_format(self, review_jsonl, tmp_path):
        """JSON array export should be parseable."""
        processor = RecordProcessor()
        exporter = DataExporter()
        records = list(processor.process_file(review_jsonl))
        out = tmp_path / "out.json"
        exporter.to_json(records, out)
        data = json.loads(out.read_text())
        assert len(data) == 30

    def test_pipeline_handles_empty_text(self, tmp_path):
        """Records with empty text should not crash the pipeline."""
        path = tmp_path / "empty_text.jsonl"
        _write_jsonl(path, [
            {"review_id": "r1", "business_id": "b1", "stars": 3.0, "text": ""},
            {"review_id": "r2", "business_id": "b1", "stars": 4.0, "text": None},
        ])
        processor = RecordProcessor()
        cleaner = TextCleaner()
        analyzer = SentimentAnalyzer()
        records = list(processor.process_file(path))
        for r in records:
            if r.get("text") is not None:
                cleaner.clean_record(r)
            analyzer.enrich_record(r)
        assert len(records) == 2

    @pytest.mark.parametrize("fmt", ["jsonl", "json"])
    def test_export_formats(self, review_jsonl, tmp_path, fmt):
        processor = RecordProcessor()
        exporter = DataExporter()
        records = list(processor.process_file(review_jsonl))
        out = tmp_path / f"out.{fmt}"
        count = exporter.export(records, out, fmt=fmt)
        assert out.exists()
        assert count == 30
