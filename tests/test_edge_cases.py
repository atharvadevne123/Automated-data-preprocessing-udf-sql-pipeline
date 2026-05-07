"""Extreme edge-case tests for split_files.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from split_files import count_lines, split_file


def _write_jsonl(path: Path, n: int) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(json.dumps({"id": i}) + "\n")


def _line_count(path: str | Path) -> int:
    return sum(1 for line in open(path, encoding="utf-8") if line.strip())


def test_split_exactly_one_line_into_one_file(tmp_path: Path) -> None:
    f = tmp_path / "one.jsonl"
    _write_jsonl(f, 1)
    outputs = split_file(f, str(tmp_path / "out_"), 1)
    assert len(outputs) == 1
    assert _line_count(outputs[0]) == 1


def test_split_large_file_preserves_all_records(tmp_path: Path) -> None:
    n = 1000
    f = tmp_path / "big.jsonl"
    _write_jsonl(f, n)
    outputs = split_file(f, str(tmp_path / "out_"), 7)
    assert sum(_line_count(o) for o in outputs) == n


def test_split_into_more_files_than_lines_caps_correctly(tmp_path: Path) -> None:
    f = tmp_path / "small.jsonl"
    _write_jsonl(f, 5)
    outputs = split_file(f, str(tmp_path / "out_"), 100)
    assert len(outputs) <= 5
    assert sum(_line_count(o) for o in outputs) == 5


def test_split_two_records_into_two_files(tmp_path: Path) -> None:
    f = tmp_path / "two.jsonl"
    _write_jsonl(f, 2)
    outputs = split_file(f, str(tmp_path / "out_"), 2)
    assert len(outputs) == 2
    assert all(_line_count(o) == 1 for o in outputs)


def test_count_lines_single_line(tmp_path: Path) -> None:
    f = tmp_path / "one.jsonl"
    f.write_text('{"id": 0}\n')
    assert count_lines(f) == 1


def test_count_lines_no_trailing_newline(tmp_path: Path) -> None:
    f = tmp_path / "nonl.jsonl"
    f.write_text('{"id": 0}')
    assert count_lines(f) == 1


def test_split_file_empty_raises_value_error(tmp_path: Path) -> None:
    f = tmp_path / "empty.jsonl"
    f.write_text("")
    with pytest.raises(ValueError, match="empty"):
        split_file(f, str(tmp_path / "out_"), 1)


@pytest.mark.parametrize("n,chunks", [(3, 1), (7, 3), (50, 10), (99, 9), (100, 10)])
def test_split_no_records_lost(tmp_path: Path, n: int, chunks: int) -> None:
    f = tmp_path / f"data_{n}_{chunks}.jsonl"
    _write_jsonl(f, n)
    effective_chunks = min(chunks, n)
    outputs = split_file(f, str(tmp_path / f"out_{n}_{chunks}_"), chunks)
    assert len(outputs) == effective_chunks
    assert sum(_line_count(o) for o in outputs) == n


def test_output_filenames_are_sequential(tmp_path: Path) -> None:
    f = tmp_path / "seq.jsonl"
    _write_jsonl(f, 5)
    outputs = split_file(f, str(tmp_path / "chunk_"), 5)
    basenames = [Path(o).name for o in outputs]
    assert basenames == [f"chunk_{i}.json" for i in range(1, 6)]
