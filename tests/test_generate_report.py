"""Tests for scripts.generate_report."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.generate_report import aggregate_jsonl, build_report, main


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


@pytest.fixture
def sample_jsonl(tmp_path):
    path = tmp_path / "reviews.jsonl"
    records = [
        {"business_id": "b1", "stars": 5.0, "useful": 1, "sentiment_label": "positive"},
        {"business_id": "b1", "stars": 4.0, "useful": 0, "sentiment_label": "positive"},
        {"business_id": "b2", "stars": 2.0, "useful": 0, "sentiment_label": "negative"},
        {"business_id": "b2", "stars": 1.0, "useful": 2, "sentiment_label": "negative"},
        {"business_id": "b3", "stars": 3.0, "useful": 0, "sentiment_label": "neutral"},
    ]
    _write_jsonl(path, records)
    return path


class TestAggregateJsonl:
    def test_returns_stats_aggregator(self, sample_jsonl):
        from pipeline.aggregator import StatsAggregator

        agg = aggregate_jsonl(sample_jsonl)
        assert isinstance(agg, StatsAggregator)

    def test_total_records_correct(self, sample_jsonl):
        agg = aggregate_jsonl(sample_jsonl)
        assert agg.global_stats().total_records == 5

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            aggregate_jsonl(tmp_path / "missing.jsonl")

    def test_empty_file_returns_zero(self, tmp_path):
        path = tmp_path / "empty.jsonl"
        path.write_text("")
        agg = aggregate_jsonl(path)
        assert agg.global_stats().total_records == 0

    def test_skips_invalid_json(self, tmp_path):
        path = tmp_path / "mixed.jsonl"
        with open(path, "w") as f:
            f.write('{"business_id":"b1","stars":3.0}\n')
            f.write("INVALID\n")
        agg = aggregate_jsonl(path)
        assert agg.global_stats().total_records == 1


class TestBuildReport:
    def test_report_has_summary_key(self, sample_jsonl):
        agg = aggregate_jsonl(sample_jsonl)
        report = build_report(agg)
        assert "summary" in report

    def test_report_has_star_distribution(self, sample_jsonl):
        agg = aggregate_jsonl(sample_jsonl)
        report = build_report(agg)
        assert "star_distribution" in report

    def test_report_has_top_businesses(self, sample_jsonl):
        agg = aggregate_jsonl(sample_jsonl)
        report = build_report(agg)
        assert "top_businesses" in report

    def test_summary_total_records(self, sample_jsonl):
        agg = aggregate_jsonl(sample_jsonl)
        report = build_report(agg)
        assert report["summary"]["total_records"] == 5

    def test_top_n_respected(self, sample_jsonl):
        agg = aggregate_jsonl(sample_jsonl)
        report = build_report(agg, top_n=1)
        assert len(report["top_businesses"]) <= 1

    def test_unique_businesses_counted(self, sample_jsonl):
        agg = aggregate_jsonl(sample_jsonl)
        report = build_report(agg)
        assert report["summary"]["unique_businesses"] == 3


class TestMain:
    def test_main_stdout_success(self, sample_jsonl, capsys):
        result = main([str(sample_jsonl)])
        assert result == 0
        out = capsys.readouterr().out
        data = json.loads(out)
        assert "summary" in data

    def test_main_writes_output_file(self, sample_jsonl, tmp_path):
        out = tmp_path / "report.json"
        result = main([str(sample_jsonl), "--output", str(out)])
        assert result == 0
        assert out.exists()
        data = json.loads(out.read_text())
        assert "summary" in data

    def test_main_returns_one_on_missing_input(self, tmp_path):
        result = main([str(tmp_path / "missing.jsonl")])
        assert result == 1

    def test_main_top_n_flag(self, sample_jsonl, capsys):
        result = main([str(sample_jsonl), "--top-n", "2"])
        assert result == 0
