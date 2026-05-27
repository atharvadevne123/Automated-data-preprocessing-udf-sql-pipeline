"""Tests verifying JSON output data quality after splitting."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from split_files import split_file


def _read_all_records(paths: list[str]) -> list[dict]:
    records = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def test_output_records_count_matches_input(sample_jsonl: Path, tmp_path: Path) -> None:
    prefix = str(tmp_path / "dq_")
    outputs = split_file(sample_jsonl, prefix, 5)
    records = _read_all_records(outputs)
    assert len(records) == 25


def test_output_records_are_valid_json(sample_jsonl: Path, tmp_path: Path) -> None:
    prefix = str(tmp_path / "valid_")
    outputs = split_file(sample_jsonl, prefix, 3)
    for path in outputs:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    obj = json.loads(line)
                    assert isinstance(obj, dict)


def test_output_records_preserve_fields(sample_jsonl: Path, tmp_path: Path) -> None:
    prefix = str(tmp_path / "fields_")
    outputs = split_file(sample_jsonl, prefix, 5)
    records = _read_all_records(outputs)
    for rec in records:
        assert "id" in rec
        assert "text" in rec
        assert "stars" in rec


def test_output_records_preserve_values(sample_jsonl: Path, tmp_path: Path) -> None:
    """Ensure record values survive the split unchanged."""
    prefix = str(tmp_path / "values_")
    outputs = split_file(sample_jsonl, prefix, 5)
    records = _read_all_records(outputs)
    ids = sorted(rec["id"] for rec in records)
    assert ids == list(range(25))


def test_no_duplicate_records(sample_jsonl: Path, tmp_path: Path) -> None:
    prefix = str(tmp_path / "dedup_")
    outputs = split_file(sample_jsonl, prefix, 5)
    records = _read_all_records(outputs)
    ids = [rec["id"] for rec in records]
    assert len(ids) == len(set(ids))


def test_output_chunks_are_non_empty(sample_jsonl: Path, tmp_path: Path) -> None:
    prefix = str(tmp_path / "nonempty_")
    outputs = split_file(sample_jsonl, prefix, 5)
    for path in outputs:
        with open(path, encoding="utf-8") as f:
            content = f.read().strip()
        assert content, f"Chunk {path} is unexpectedly empty"


def test_output_ordering_preserved(tiny_jsonl: Path, tmp_path: Path) -> None:
    """Records should appear in their original order across chunks."""
    prefix = str(tmp_path / "order_")
    outputs = split_file(tiny_jsonl, prefix, 3)
    records = _read_all_records(outputs)
    assert [rec["id"] for rec in records] == [0, 1, 2]


@pytest.mark.parametrize("num_chunks", [1, 2, 5, 10, 25])
def test_total_records_stable_across_chunk_counts(sample_jsonl: Path, tmp_path: Path, num_chunks: int) -> None:
    prefix = str(tmp_path / f"c{num_chunks}_")
    outputs = split_file(sample_jsonl, prefix, num_chunks)
    records = _read_all_records(outputs)
    assert len(records) == 25
