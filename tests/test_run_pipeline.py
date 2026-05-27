"""Tests for scripts/run_pipeline.py orchestration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.run_pipeline import run_pipeline


@pytest.fixture
def sample_jsonl(tmp_path) -> Path:
    data = [
        {
            "review_id": f"r{i}",
            "user_id": "u1",
            "business_id": "b1",
            "stars": (i % 5) + 1,
            "text": f"Review number {i}",
            "date": "2023-01-01",
        }
        for i in range(10)
    ]
    f = tmp_path / "reviews.jsonl"
    f.write_text("\n".join(json.dumps(d) for d in data) + "\n")
    return f


class TestRunPipeline:
    def test_basic_run_returns_summary(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "output"
        result = run_pipeline(sample_jsonl, output_dir)
        assert "records_written" in result
        assert "elapsed_sec" in result

    def test_output_file_created(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "output"
        result = run_pipeline(sample_jsonl, output_dir)
        assert Path(result["output"]).exists()

    def test_all_records_written_jsonl(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "out"
        result = run_pipeline(sample_jsonl, output_dir, output_format="jsonl")
        assert result["records_written"] == 10

    def test_min_stars_filter(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "out"
        result = run_pipeline(sample_jsonl, output_dir, min_stars=4.0)
        assert result["records_written"] < 10

    def test_max_records_limit(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "out"
        result = run_pipeline(sample_jsonl, output_dir, max_records=3)
        assert result["records_written"] == 3

    def test_sample_rate(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "out"
        result = run_pipeline(sample_jsonl, output_dir, sample_rate=0.5)
        assert result["records_written"] <= 10

    def test_json_format(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "out"
        result = run_pipeline(sample_jsonl, output_dir, output_format="json")
        out_path = Path(result["output"])
        loaded = json.loads(out_path.read_text())
        assert isinstance(loaded, list)

    def test_csv_format(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "out"
        result = run_pipeline(sample_jsonl, output_dir, output_format="csv")
        out_path = Path(result["output"])
        content = out_path.read_text()
        assert "review_id" in content

    def test_cleaner_stats_included(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "out"
        result = run_pipeline(sample_jsonl, output_dir)
        assert "cleaner_stats" in result

    def test_sentiment_labels_in_output(self, sample_jsonl, tmp_path):
        output_dir = tmp_path / "out"
        result = run_pipeline(sample_jsonl, output_dir, output_format="jsonl")
        out_path = Path(result["output"])
        lines = [json.loads(ln) for ln in out_path.read_text().strip().split("\n")]
        assert all("sentiment_label" in r for r in lines)

    @pytest.mark.parametrize("fmt", ["jsonl", "json", "csv"])
    def test_all_formats_produce_output(self, sample_jsonl, tmp_path, fmt):
        output_dir = tmp_path / f"out_{fmt}"
        result = run_pipeline(sample_jsonl, output_dir, output_format=fmt)
        assert Path(result["output"]).exists()
        assert result["records_written"] > 0
