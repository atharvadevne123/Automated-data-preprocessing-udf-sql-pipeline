"""Tests for analyze_file() output_format parameter and --output-format CLI flag."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.analyze_sentiment import analyze_file, main


@pytest.fixture
def sample_jsonl(tmp_path: Path) -> Path:
    filepath = tmp_path / "reviews.jsonl"
    records = [
        {"text": "This is absolutely wonderful!", "id": i}
        for i in range(5)
    ]
    filepath.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    return filepath


class TestAnalyzeFileOutputFormat:
    def test_jsonl_format_produces_one_line_per_record(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        analyze_file(sample_jsonl, out, output_format="jsonl")
        lines = [ln for ln in out.read_text().splitlines() if ln.strip()]
        assert len(lines) == 5

    def test_jsonl_each_line_is_valid_json(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        analyze_file(sample_jsonl, out, output_format="jsonl")
        for line in out.read_text().splitlines():
            if line.strip():
                json.loads(line)

    def test_json_format_produces_array(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.json"
        analyze_file(sample_jsonl, out, output_format="json")
        loaded = json.loads(out.read_text())
        assert isinstance(loaded, list)
        assert len(loaded) == 5

    def test_json_format_contains_sentiment_label(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.json"
        analyze_file(sample_jsonl, out, output_format="json")
        records = json.loads(out.read_text())
        assert all("sentiment_label" in r for r in records)

    def test_jsonl_format_contains_sentiment_label(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        analyze_file(sample_jsonl, out, output_format="jsonl")
        records = [json.loads(ln) for ln in out.read_text().splitlines() if ln.strip()]
        assert all("sentiment_label" in r for r in records)

    def test_unsupported_format_raises_value_error(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.xml"
        with pytest.raises(ValueError, match="Unsupported output_format"):
            analyze_file(sample_jsonl, out, output_format="xml")

    def test_counts_match_record_count(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.jsonl"
        counts = analyze_file(sample_jsonl, out, output_format="jsonl")
        assert counts["total"] == 5
        assert counts["positive"] + counts["negative"] + counts["neutral"] == 5

    def test_json_format_counts_match(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.json"
        counts = analyze_file(sample_jsonl, out, output_format="json")
        assert counts["total"] == 5

    def test_nonexistent_input_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            analyze_file(tmp_path / "ghost.jsonl", tmp_path / "out.jsonl")

    def test_output_dir_created_automatically(self, sample_jsonl, tmp_path):
        nested_out = tmp_path / "nested" / "dir" / "out.jsonl"
        analyze_file(sample_jsonl, nested_out, output_format="jsonl")
        assert nested_out.exists()


class TestAnalyzeSentimentCLI:
    def test_cli_default_format_is_jsonl(self, sample_jsonl, tmp_path):
        out = tmp_path / "cli_out.jsonl"
        ret = main([str(sample_jsonl), str(out)])
        assert ret == 0
        lines = [ln for ln in out.read_text().splitlines() if ln.strip()]
        assert len(lines) == 5

    def test_cli_json_format(self, sample_jsonl, tmp_path):
        out = tmp_path / "cli_out.json"
        ret = main([str(sample_jsonl), str(out), "--output-format", "json"])
        assert ret == 0
        loaded = json.loads(out.read_text())
        assert isinstance(loaded, list)

    def test_cli_missing_file_returns_1(self, tmp_path):
        ret = main([str(tmp_path / "ghost.jsonl"), str(tmp_path / "out.jsonl")])
        assert ret == 1

    def test_cli_invalid_format_exits_nonzero(self, sample_jsonl, tmp_path):
        out = tmp_path / "out.xml"
        with pytest.raises(SystemExit):
            main([str(sample_jsonl), str(out), "--output-format", "xml"])
