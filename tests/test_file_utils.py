"""Tests for utils/file_utils.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.file_utils import (
    ensure_dir,
    file_line_count,
    file_size_mb,
    iter_jsonl,
    list_jsonl_files,
    safe_read_json,
    write_jsonl,
)


class TestEnsureDir:
    def test_creates_directory(self, tmp_path: Path) -> None:
        new_dir = tmp_path / "a" / "b" / "c"
        result = ensure_dir(new_dir)
        assert result.exists()
        assert result.is_dir()

    def test_returns_path(self, tmp_path: Path) -> None:
        result = ensure_dir(tmp_path / "sub")
        assert isinstance(result, Path)

    def test_idempotent(self, tmp_path: Path) -> None:
        d = tmp_path / "existing"
        ensure_dir(d)
        ensure_dir(d)
        assert d.exists()


class TestFileLineCount:
    def test_counts_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "test.txt"
        f.write_text("line1\nline2\nline3\n")
        assert file_line_count(f) == 3

    def test_empty_file(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.txt"
        f.write_text("")
        assert file_line_count(f) == 0

    def test_single_line_no_newline(self, tmp_path: Path) -> None:
        f = tmp_path / "no_nl.txt"
        f.write_bytes(b"no newline")
        assert file_line_count(f) == 0


class TestIterJsonl:
    def _write_jsonl(self, tmp_path: Path, records: list[dict]) -> Path:
        f = tmp_path / "data.jsonl"
        f.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        return f

    def test_basic(self, tmp_path: Path) -> None:
        f = self._write_jsonl(tmp_path, [{"a": 1}, {"b": 2}])
        result = list(iter_jsonl(f))
        assert result == [{"a": 1}, {"b": 2}]

    def test_skips_blank_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "blanks.jsonl"
        f.write_text('{"a":1}\n\n{"b":2}\n')
        result = list(iter_jsonl(f))
        assert len(result) == 2

    def test_skips_invalid_json_by_default(self, tmp_path: Path) -> None:
        f = tmp_path / "bad.jsonl"
        f.write_text('{"a":1}\nNOT_JSON\n{"c":3}\n')
        result = list(iter_jsonl(f))
        assert len(result) == 2

    def test_raises_on_invalid_when_strict(self, tmp_path: Path) -> None:
        f = tmp_path / "bad2.jsonl"
        f.write_text("NOT_JSON\n")
        with pytest.raises(json.JSONDecodeError):
            list(iter_jsonl(f, skip_errors=False))

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises((FileNotFoundError, OSError)):
            list(iter_jsonl(tmp_path / "nonexistent.jsonl"))


class TestWriteJsonl:
    def test_writes_records(self, tmp_path: Path) -> None:
        records = [{"id": 1}, {"id": 2}]
        path = tmp_path / "out.jsonl"
        count = write_jsonl(records, path)
        assert count == 2
        assert path.exists()
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_creates_parent_dir(self, tmp_path: Path) -> None:
        path = tmp_path / "sub" / "out.jsonl"
        write_jsonl([{"x": 1}], path)
        assert path.exists()

    def test_returns_count(self, tmp_path: Path) -> None:
        records = [{"a": i} for i in range(5)]
        count = write_jsonl(records, tmp_path / "r.jsonl")
        assert count == 5


class TestFileSizeMb:
    def test_positive_size(self, tmp_path: Path) -> None:
        f = tmp_path / "big.txt"
        f.write_bytes(b"x" * 1024 * 1024)
        assert file_size_mb(f) == pytest.approx(1.0, rel=0.01)

    def test_nonexistent_returns_zero(self, tmp_path: Path) -> None:
        assert file_size_mb(tmp_path / "nope.txt") == 0.0


class TestListJsonlFiles:
    def test_finds_files(self, tmp_path: Path) -> None:
        (tmp_path / "a.jsonl").write_text("")
        (tmp_path / "b.jsonl").write_text("")
        (tmp_path / "c.txt").write_text("")
        result = list_jsonl_files(tmp_path)
        assert len(result) == 2
        assert all(f.suffix == ".jsonl" for f in result)

    def test_recursive(self, tmp_path: Path) -> None:
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "deep.jsonl").write_text("")
        result = list_jsonl_files(tmp_path, recursive=True)
        names = [f.name for f in result]
        assert "deep.jsonl" in names


class TestSafeReadJson:
    def test_reads_valid_json(self, tmp_path: Path) -> None:
        f = tmp_path / "data.json"
        f.write_text('{"key": "value"}')
        result = safe_read_json(f)
        assert result == {"key": "value"}

    def test_returns_default_on_missing_file(self, tmp_path: Path) -> None:
        result = safe_read_json(tmp_path / "nope.json", default={})
        assert result == {}

    def test_returns_default_on_invalid_json(self, tmp_path: Path) -> None:
        f = tmp_path / "bad.json"
        f.write_text("NOT JSON")
        result = safe_read_json(f, default=None)
        assert result is None
