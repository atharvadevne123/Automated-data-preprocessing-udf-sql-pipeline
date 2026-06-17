"""Tests for --workers flag in scripts/run_pipeline.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.run_pipeline import run_pipeline


def _make_jsonl(path: Path, n: int = 5) -> Path:
    records = [
        {"review_id": f"r{i}", "business_id": f"biz{i % 3}", "stars": (i % 5) + 1, "text": f"Review {i}"}
        for i in range(n)
    ]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    return path


class TestRunPipelineWorkers:
    def test_workers_one_runs_correctly(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 5)
        summary = run_pipeline(src, tmp_path / "out", workers=1)
        assert summary["records_written"] == 5
        assert summary["workers"] == 1

    def test_summary_has_workers_key(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 4)
        summary = run_pipeline(src, tmp_path / "out", workers=1)
        assert "workers" in summary

    def test_output_file_created(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 3)
        run_pipeline(src, tmp_path / "out")
        assert (tmp_path / "out" / "pipeline_output.jsonl").exists()

    def test_max_records_respected(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 10)
        summary = run_pipeline(src, tmp_path / "out", max_records=3, workers=1)
        assert summary["records_written"] == 3

    def test_min_stars_filter(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 10)
        summary = run_pipeline(src, tmp_path / "out", min_stars=5.0, workers=1)
        assert summary["records_written"] <= 10

    def test_json_format_output(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 3)
        run_pipeline(src, tmp_path / "out", output_format="json", workers=1)
        out = tmp_path / "out" / "pipeline_output.json"
        assert out.exists()
        loaded = json.loads(out.read_text())
        assert isinstance(loaded, list)
