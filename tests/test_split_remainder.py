"""Tests for the remainder-distribution logic in split_file."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from split_files import split_file


def _write_jsonl(path: Path, n: int) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(json.dumps({"id": i}) + "\n")


def _line_count(path: str | Path) -> int:
    return sum(1 for line in open(path, encoding="utf-8") if line.strip())


@pytest.mark.parametrize(
    "total,chunks,expected_last",
    [
        (10, 3, 4),   # 10 / 3 = 3 r 1; last gets 3 + 1 = 4
        (11, 4, 5),   # 11 / 4 = 2 r 3; last gets 2 + 3 = 5
        (7, 3, 3),    # 7 / 3 = 2 r 1; last gets 2 + 1 = 3
        (25, 4, 7),   # 25 / 4 = 6 r 1; last gets 6 + 1 = 7
    ],
)
def test_last_chunk_gets_remainder(
    tmp_path: Path, total: int, chunks: int, expected_last: int
) -> None:
    f = tmp_path / f"r_{total}_{chunks}.jsonl"
    _write_jsonl(f, total)
    outputs = split_file(f, str(tmp_path / "out_"), chunks)
    last_count = _line_count(outputs[-1])
    assert last_count == expected_last, f"Expected {expected_last}, got {last_count}"


@pytest.mark.parametrize("total,chunks", [(20, 4), (30, 5), (100, 10)])
def test_even_split_all_chunks_equal(tmp_path: Path, total: int, chunks: int) -> None:
    f = tmp_path / f"even_{total}_{chunks}.jsonl"
    _write_jsonl(f, total)
    outputs = split_file(f, str(tmp_path / "out_"), chunks)
    sizes = [_line_count(o) for o in outputs]
    per_chunk = total // chunks
    assert all(s == per_chunk for s in sizes), f"Expected all {per_chunk}, got {sizes}"


def test_remainder_goes_entirely_to_last_chunk(tmp_path: Path) -> None:
    f = tmp_path / "r.jsonl"
    _write_jsonl(f, 13)  # 13/5 = 2r3; last = 2+3 = 5
    outputs = split_file(f, str(tmp_path / "out_"), 5)
    sizes = [_line_count(o) for o in outputs]
    assert sizes[-1] == 5
    assert all(s == 2 for s in sizes[:-1])
