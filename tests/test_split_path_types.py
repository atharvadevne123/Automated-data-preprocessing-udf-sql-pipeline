"""Tests verifying split_file and count_lines handle various path types."""

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


def test_split_file_accepts_path_object(tmp_path: Path) -> None:
    f = tmp_path / "data.jsonl"
    _write_jsonl(f, 10)
    outputs = split_file(f, str(tmp_path / "out_"), 2)
    assert sum(_line_count(o) for o in outputs) == 10


def test_count_lines_accepts_path_object(tmp_path: Path) -> None:
    f = tmp_path / "data.jsonl"
    _write_jsonl(f, 15)
    assert count_lines(f) == 15


def test_split_file_output_prefix_in_subdir(tmp_path: Path) -> None:
    f = tmp_path / "data.jsonl"
    _write_jsonl(f, 9)
    subdir = tmp_path / "sub" / "dir"
    subdir.mkdir(parents=True)
    outputs = split_file(f, str(subdir / "chunk_"), 3)
    assert len(outputs) == 3
    assert all(Path(o).parent == subdir for o in outputs)


def test_split_file_creates_files_with_json_extension(tmp_path: Path) -> None:
    f = tmp_path / "data.jsonl"
    _write_jsonl(f, 6)
    outputs = split_file(f, str(tmp_path / "part_"), 3)
    assert all(o.endswith(".json") for o in outputs)


@pytest.mark.parametrize("n,chunks", [(3, 3), (10, 2), (50, 5)])
def test_output_files_exist_after_split(tmp_path: Path, n: int, chunks: int) -> None:
    f = tmp_path / f"d_{n}.jsonl"
    _write_jsonl(f, n)
    outputs = split_file(f, str(tmp_path / "o_"), chunks)
    for path in outputs:
        assert Path(path).exists(), f"Output file missing: {path}"
