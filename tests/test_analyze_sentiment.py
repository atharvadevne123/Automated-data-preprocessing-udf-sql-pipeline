"""Tests for scripts.analyze_sentiment."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.analyze_sentiment import analyze_file, main


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


@pytest.fixture
def sample_jsonl(tmp_path):
    path = tmp_path / "reviews.jsonl"
    _write_jsonl(path, [
        {"review_id": "r1", "text": "Great food!", "stars": 5},
        {"review_id": "r2", "text": "Terrible service", "stars": 1},
        {"review_id": "r3", "text": "It was okay", "stars": 3},
    ])
    return path


class TestAnalyzeFile:
    def test_returns_counts_dict(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        counts = analyze_file(sample_jsonl, out)
        assert "total" in counts
        assert counts["total"] == 3

    def test_output_file_created(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        analyze_file(sample_jsonl, out)
        assert out.exists()

    def test_output_enriched_with_sentiment(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        analyze_file(sample_jsonl, out)
        with open(out) as f:
            first = json.loads(f.readline())
        assert "sentiment_label" in first
        assert "sentiment_polarity" in first

    def test_creates_parent_dirs(self, sample_jsonl, tmp_path):
        out = tmp_path / "subdir" / "nested" / "out.jsonl"
        analyze_file(sample_jsonl, out)
        assert out.exists()

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            analyze_file(Path("/nonexistent/file.jsonl"), tmp_path / "out.jsonl")

    def test_skips_invalid_json_lines(self, tmp_path):
        path = tmp_path / "mixed.jsonl"
        with open(path, "w") as f:
            f.write('{"text": "good"}\n')
            f.write("NOT JSON\n")
            f.write('{"text": "also good"}\n')
        out = tmp_path / "out.jsonl"
        counts = analyze_file(path, out)
        assert counts["total"] == 2

    def test_empty_file_returns_zero_counts(self, tmp_path):
        path = tmp_path / "empty.jsonl"
        path.write_text("")
        out = tmp_path / "out.jsonl"
        counts = analyze_file(path, out)
        assert counts["total"] == 0

    def test_custom_text_field(self, tmp_path):
        path = tmp_path / "custom.jsonl"
        _write_jsonl(path, [{"review": "excellent", "stars": 5}])
        out = tmp_path / "out.jsonl"
        analyze_file(path, out, text_field="review")
        with open(out) as f:
            record = json.loads(f.readline())
        assert "sentiment_label" in record


class TestMain:
    def test_main_returns_zero_on_success(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        result = main([str(sample_jsonl), str(out)])
        assert result == 0

    def test_main_returns_one_on_missing_file(self, tmp_path):
        result = main(["/nonexistent/file.jsonl", str(tmp_path / "out.jsonl")])
        assert result == 1

    def test_main_creates_output(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        main([str(sample_jsonl), str(out)])
        assert out.exists()
