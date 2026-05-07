"""Tests for split_files.py with the large_jsonl fixture."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from split_files import split_file


def _line_count(path: str | Path) -> int:
    return sum(1 for line in open(path, encoding="utf-8") if line.strip())


def _read_all(paths: list[str]) -> list[dict]:
    records = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            records.extend(json.loads(l) for l in f if l.strip())
    return records


def test_large_jsonl_split_into_10_chunks(large_jsonl: Path, tmp_path: Path) -> None:
    outputs = split_file(large_jsonl, str(tmp_path / "l_"), 10)
    assert len(outputs) == 10
    assert sum(_line_count(o) for o in outputs) == 100


def test_large_jsonl_all_records_preserved(large_jsonl: Path, tmp_path: Path) -> None:
    outputs = split_file(large_jsonl, str(tmp_path / "l_"), 7)
    records = _read_all(outputs)
    ids = sorted(r["id"] for r in records)
    assert ids == list(range(100))


def test_large_jsonl_no_duplicates(large_jsonl: Path, tmp_path: Path) -> None:
    outputs = split_file(large_jsonl, str(tmp_path / "l_"), 5)
    ids = [r["id"] for r in _read_all(outputs)]
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("chunks", [1, 4, 10, 20, 50, 100])
def test_large_jsonl_various_chunk_counts(
    large_jsonl: Path, tmp_path: Path, chunks: int
) -> None:
    outputs = split_file(large_jsonl, str(tmp_path / f"c{chunks}_"), chunks)
    assert sum(_line_count(o) for o in outputs) == 100
    assert len(outputs) == chunks


def test_large_jsonl_chunk_sizes_balanced(large_jsonl: Path, tmp_path: Path) -> None:
    outputs = split_file(large_jsonl, str(tmp_path / "bal_"), 10)
    sizes = [_line_count(o) for o in outputs]
    assert min(sizes) >= 9  # 100/10 = 10, remainder goes to last
    assert max(sizes) <= 10
