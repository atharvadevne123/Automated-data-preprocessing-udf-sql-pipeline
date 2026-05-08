"""Tests for split_file_stream() and estimate_chunks_for_size() in split_files."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from split_files import estimate_chunks_for_size, split_file_stream


def _make_jsonl(n: int) -> Path:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    for i in range(n):
        f.write(json.dumps({"id": i}) + "\n")
    f.flush()
    return Path(f.name)


class TestSplitFileStream:
    def test_returns_correct_number_of_chunks(self):
        path = _make_jsonl(10)
        chunks = split_file_stream(path, 5)
        assert len(chunks) == 5

    def test_total_lines_preserved(self):
        path = _make_jsonl(20)
        chunks = split_file_stream(path, 4)
        total = sum(len(c) for c in chunks)
        assert total == 20

    def test_single_chunk_returns_all_lines(self):
        path = _make_jsonl(7)
        chunks = split_file_stream(path, 1)
        assert len(chunks) == 1
        assert len(chunks[0]) == 7

    def test_num_files_capped_at_total_lines(self):
        path = _make_jsonl(3)
        chunks = split_file_stream(path, 100)
        assert len(chunks) == 3

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            split_file_stream(Path("/no/such/file.jsonl"), 2)

    def test_empty_file_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = Path(f.name)
        with pytest.raises(ValueError, match="empty"):
            split_file_stream(path, 2)

    def test_num_files_zero_raises(self):
        path = _make_jsonl(5)
        with pytest.raises(ValueError):
            split_file_stream(path, 0)

    def test_chunks_are_lists_of_strings(self):
        path = _make_jsonl(6)
        chunks = split_file_stream(path, 3)
        for chunk in chunks:
            assert all(isinstance(line, str) for line in chunk)

    @pytest.mark.parametrize("n,k", [(1, 1), (5, 5), (10, 2), (10, 10), (100, 7)])
    def test_various_sizes_and_chunks(self, n, k):
        path = _make_jsonl(n)
        chunks = split_file_stream(path, k)
        total = sum(len(c) for c in chunks)
        assert total == n


class TestEstimateChunksForSize:
    def test_large_file_divided_correctly(self, tmp_path):
        path = tmp_path / "big.jsonl"
        # Write ~1MB of data
        with open(path, "w") as f:
            for i in range(10000):
                f.write(json.dumps({"id": i, "text": "x" * 100}) + "\n")
        size_mb = path.stat().st_size / (1024 * 1024)
        n = estimate_chunks_for_size(path, target_size_mb=size_mb / 2)
        assert n >= 2

    def test_file_smaller_than_target_returns_one(self, tmp_path):
        path = tmp_path / "tiny.jsonl"
        path.write_text('{"id":1}\n')
        n = estimate_chunks_for_size(path, target_size_mb=100.0)
        assert n == 1

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            estimate_chunks_for_size(Path("/no/such.jsonl"), 1.0)

    def test_target_zero_raises(self, tmp_path):
        path = tmp_path / "f.jsonl"
        path.write_text('{"id":1}\n')
        with pytest.raises(ValueError):
            estimate_chunks_for_size(path, target_size_mb=0.0)

    def test_target_negative_raises(self, tmp_path):
        path = tmp_path / "f.jsonl"
        path.write_text('{"id":1}\n')
        with pytest.raises(ValueError):
            estimate_chunks_for_size(path, target_size_mb=-1.0)

    def test_always_returns_at_least_one(self, tmp_path):
        path = tmp_path / "f.jsonl"
        path.write_text('{"id":1}\n')
        assert estimate_chunks_for_size(path, target_size_mb=0.001) >= 1
