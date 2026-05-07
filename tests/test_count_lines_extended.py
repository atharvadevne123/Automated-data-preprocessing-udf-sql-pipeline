"""Extended tests for split_files.count_lines."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from split_files import count_lines


def _write_lines(path: Path, n: int) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(json.dumps({"id": i}) + "\n")


@pytest.mark.parametrize("n", [1, 5, 10, 100, 500])
def test_count_lines_various_sizes(tmp_path: Path, n: int) -> None:
    f = tmp_path / f"data_{n}.jsonl"
    _write_lines(f, n)
    assert count_lines(f) == n


def test_count_lines_unicode_content(tmp_path: Path) -> None:
    f = tmp_path / "unicode.jsonl"
    with open(f, "w", encoding="utf-8") as fh:
        fh.write('{"text": "こんにちは"}\n')
        fh.write('{"text": "Привет"}\n')
        fh.write('{"text": "مرحبا"}\n')
    assert count_lines(f) == 3


def test_count_lines_raises_on_missing_file(tmp_path: Path) -> None:
    with pytest.raises(OSError):
        count_lines(tmp_path / "nonexistent.jsonl")


def test_count_lines_with_blank_lines(tmp_path: Path) -> None:
    f = tmp_path / "blanks.jsonl"
    f.write_text('{"id": 1}\n\n{"id": 2}\n')
    assert count_lines(f) == 3  # blank lines count as lines


def test_count_lines_large_records(tmp_path: Path) -> None:
    f = tmp_path / "large_records.jsonl"
    with open(f, "w", encoding="utf-8") as fh:
        for i in range(10):
            rec = {"id": i, "data": "x" * 10000}
            fh.write(json.dumps(rec) + "\n")
    assert count_lines(f) == 10


def test_count_lines_crlf_line_endings(tmp_path: Path) -> None:
    f = tmp_path / "crlf.jsonl"
    with open(f, "wb") as fh:
        for i in range(5):
            fh.write((json.dumps({"id": i}) + "\r\n").encode("utf-8"))
    assert count_lines(f) == 5
