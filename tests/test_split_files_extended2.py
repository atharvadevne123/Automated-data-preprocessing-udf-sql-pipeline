"""Additional parametrized tests for split_files.py core functions."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from split_files import count_lines, get_file_size_mb, split_file


def _make_jsonl(n: int) -> Path:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    for i in range(n):
        f.write(json.dumps({"id": i, "text": f"review {i}" * 5}) + "\n")
    f.flush()
    return Path(f.name)


class TestCountLinesExtended:
    @pytest.mark.parametrize("n", [0, 1, 5, 100, 1000])
    def test_count_lines_various_sizes(self, n):
        path = _make_jsonl(n)
        assert count_lines(path) == n

    def test_count_lines_nonexistent_raises(self):
        with pytest.raises(OSError):
            count_lines(Path("/no/such/file.jsonl"))

    def test_count_lines_with_blank_lines(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write("line1\n\nline2\n\n")
            path = Path(f.name)
        assert count_lines(path) == 4


class TestGetFileSizeMb:
    def test_existing_file_returns_positive(self, tmp_path):
        p = tmp_path / "f.txt"
        p.write_text("hello world " * 1000)
        assert get_file_size_mb(p) > 0

    def test_missing_file_returns_zero(self):
        assert get_file_size_mb(Path("/no/such/file")) == 0.0

    def test_empty_file_returns_zero(self, tmp_path):
        p = tmp_path / "empty.txt"
        p.write_text("")
        assert get_file_size_mb(p) == pytest.approx(0.0, abs=1e-6)


class TestSplitFileExtended:
    def test_output_files_count_matches_num_files(self, tmp_path):
        path = _make_jsonl(20)
        outputs = split_file(path, str(tmp_path / "chunk_"), 5)
        assert len(outputs) == 5

    def test_all_records_preserved(self, tmp_path):
        n = 37
        path = _make_jsonl(n)
        outputs = split_file(path, str(tmp_path / "chunk_"), 7)
        total = sum(count_lines(Path(o)) for o in outputs)
        assert total == n

    def test_capped_when_files_exceed_lines(self, tmp_path):
        path = _make_jsonl(5)
        outputs = split_file(path, str(tmp_path / "chunk_"), 100)
        assert len(outputs) == 5

    def test_single_file_split(self, tmp_path):
        path = _make_jsonl(10)
        outputs = split_file(path, str(tmp_path / "chunk_"), 1)
        assert len(outputs) == 1
        assert count_lines(Path(outputs[0])) == 10

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            split_file(Path("/no/such/file.jsonl"), str(tmp_path / "c_"), 3)

    def test_num_files_zero_raises(self, tmp_path):
        path = _make_jsonl(5)
        with pytest.raises(ValueError):
            split_file(path, str(tmp_path / "c_"), 0)

    def test_empty_file_raises(self, tmp_path):
        path = tmp_path / "empty.jsonl"
        path.write_text("")
        with pytest.raises(ValueError, match="empty"):
            split_file(path, str(tmp_path / "c_"), 3)

    @pytest.mark.parametrize("n,chunks", [
        (1, 1), (10, 2), (10, 5), (100, 10), (50, 7),
    ])
    def test_various_n_and_chunks(self, tmp_path, n, chunks):
        path = _make_jsonl(n)
        outputs = split_file(path, str(tmp_path / "c_"), chunks)
        actual_chunks = min(chunks, n)
        assert len(outputs) == actual_chunks
        total = sum(count_lines(Path(o)) for o in outputs)
        assert total == n
