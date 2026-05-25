"""Tests for RecordProcessor.process_files_parallel()."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.processor import RecordProcessor


@pytest.fixture
def tmp_jsonl_files(tmp_path):
    """Create two small JSONL files in tmp_path."""
    f1 = tmp_path / "a.jsonl"
    f2 = tmp_path / "b.jsonl"
    f1.write_text(json.dumps({"id": 1, "stars": 5}) + "\n" + json.dumps({"id": 2, "stars": 4}) + "\n")
    f2.write_text(json.dumps({"id": 3, "stars": 3}) + "\n")
    return [f1, f2]


class TestProcessFilesParallel:
    def test_returns_all_records(self, tmp_jsonl_files):
        proc = RecordProcessor()
        results = proc.process_files_parallel(tmp_jsonl_files)
        assert len(results) == 3

    def test_ids_present(self, tmp_jsonl_files):
        proc = RecordProcessor()
        results = proc.process_files_parallel(tmp_jsonl_files)
        ids = {r["id"] for r in results}
        assert ids == {1, 2, 3}

    def test_with_filter(self, tmp_jsonl_files):
        proc = RecordProcessor(filters=[lambda r: r.get("stars", 0) >= 4])
        results = proc.process_files_parallel(tmp_jsonl_files)
        assert all(r["stars"] >= 4 for r in results)

    def test_empty_paths(self):
        proc = RecordProcessor()
        results = proc.process_files_parallel([])
        assert results == []

    def test_max_workers_one(self, tmp_jsonl_files):
        proc = RecordProcessor()
        results = proc.process_files_parallel(tmp_jsonl_files, max_workers=1)
        assert len(results) == 3

    def test_nonexistent_file_skipped(self, tmp_path, tmp_jsonl_files):
        bad = tmp_path / "nonexistent.jsonl"
        proc = RecordProcessor()
        results = proc.process_files_parallel([*tmp_jsonl_files, bad])
        assert len(results) == 3

    @pytest.mark.parametrize("max_workers", [1, 2, 4])
    def test_parametrized_workers(self, tmp_jsonl_files, max_workers):
        proc = RecordProcessor()
        results = proc.process_files_parallel(tmp_jsonl_files, max_workers=max_workers)
        assert len(results) == 3
