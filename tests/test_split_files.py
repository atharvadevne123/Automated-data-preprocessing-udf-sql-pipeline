"""Tests for split_files.py"""

import json
from pathlib import Path

import pytest

from split_files import count_lines, split_file


@pytest.fixture
def sample_json_file(tmp_path: Path) -> Path:
    """25-record newline-delimited JSON file."""
    filepath = tmp_path / "sample.json"
    with open(filepath, "w", encoding="utf-8") as f:
        for i in range(25):
            f.write(json.dumps({"id": i, "text": f"review {i}"}) + "\n")
    return filepath


def _line_count(path: str) -> int:
    return sum(1 for _ in open(path, encoding="utf-8"))


def test_count_lines(sample_json_file: Path) -> None:
    assert count_lines(sample_json_file) == 25


def test_split_file_basic(sample_json_file: Path, tmp_path: Path) -> None:
    prefix = str(tmp_path / "out_")
    outputs = split_file(sample_json_file, prefix, 5)
    assert len(outputs) == 5
    assert sum(_line_count(f) for f in outputs) == 25


def test_split_file_remainder(sample_json_file: Path, tmp_path: Path) -> None:
    """Remainder lines must not be silently dropped (25 / 4 = 6 r 1)."""
    prefix = str(tmp_path / "rem_")
    outputs = split_file(sample_json_file, prefix, 4)
    assert len(outputs) == 4
    counts = [_line_count(f) for f in outputs]
    assert sum(counts) == 25
    assert counts[-1] == counts[0] + 1  # last file gets +1 remainder


def test_split_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        split_file(Path("nonexistent.json"), str(tmp_path / "out_"), 5)


def test_split_file_invalid_num_files(sample_json_file: Path, tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        split_file(sample_json_file, str(tmp_path / "out_"), 0)


def test_split_single_file(sample_json_file: Path, tmp_path: Path) -> None:
    prefix = str(tmp_path / "single_")
    outputs = split_file(sample_json_file, prefix, 1)
    assert len(outputs) == 1
    assert _line_count(outputs[0]) == 25


def test_split_more_files_than_lines(tmp_path: Path) -> None:
    """Splitting 3 lines into 5 files: last files may be empty but no crash."""
    filepath = tmp_path / "tiny.json"
    with open(filepath, "w", encoding="utf-8") as f:
        for i in range(3):
            f.write(json.dumps({"id": i}) + "\n")
    prefix = str(tmp_path / "tiny_")
    outputs = split_file(filepath, prefix, 3)
    assert sum(_line_count(f) for f in outputs) == 3


def test_split_preserves_valid_json(sample_json_file: Path, tmp_path: Path) -> None:
    prefix = str(tmp_path / "valid_")
    outputs = split_file(sample_json_file, prefix, 3)
    for path in outputs:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    json.loads(line)
