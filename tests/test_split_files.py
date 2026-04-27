"""Tests for split_files.py"""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from split_files import count_lines, main, split_file


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
    """Splitting 3 lines into 3 files: each file gets exactly one line."""
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


# ---------- New tests ----------


def test_split_num_files_exceeds_lines_is_capped(tmp_path: Path) -> None:
    """Requesting more files than lines caps output to total_lines files."""
    filepath = tmp_path / "small.json"
    with open(filepath, "w", encoding="utf-8") as f:
        for i in range(3):
            f.write(json.dumps({"id": i}) + "\n")
    prefix = str(tmp_path / "capped_")
    outputs = split_file(filepath, prefix, 10)
    # Should cap to 3 files, one line each
    assert len(outputs) == 3
    assert all(_line_count(f) == 1 for f in outputs)
    assert sum(_line_count(f) for f in outputs) == 3


def test_split_output_dir_creates_directory(sample_json_file: Path, tmp_path: Path) -> None:
    """--output-dir creates the directory if it does not exist."""
    out_dir = tmp_path / "nested" / "chunks"
    result = subprocess.run(
        [
            sys.executable,
            "split_files.py",
            str(sample_json_file),
            "--num-files",
            "5",
            "--output-dir",
            str(out_dir),
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, result.stderr
    assert out_dir.is_dir()
    chunks = list(out_dir.glob("split_file_*.json"))
    assert len(chunks) == 5
    assert sum(_line_count(str(c)) for c in chunks) == 25


def test_split_output_dir_combined_with_prefix(
    sample_json_file: Path, tmp_path: Path
) -> None:
    """--output-dir and --output-prefix work together correctly."""
    out_dir = tmp_path / "out"
    result = subprocess.run(
        [
            sys.executable,
            "split_files.py",
            str(sample_json_file),
            "--num-files",
            "2",
            "--output-dir",
            str(out_dir),
            "--output-prefix",
            "part_",
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, result.stderr
    assert (out_dir / "part_1.json").exists()
    assert (out_dir / "part_2.json").exists()


# ---------- main() coverage tests ----------


def test_main_missing_input_exits_1(tmp_path: Path) -> None:
    """main() exits with code 1 when the input file is missing."""
    with patch("sys.argv", ["split_files.py", str(tmp_path / "ghost.json")]):
        with pytest.raises(SystemExit) as exc_info:
            main()
    assert exc_info.value.code == 1


def test_main_runs_with_output_dir(sample_json_file: Path, tmp_path: Path) -> None:
    """main() creates output chunks in the specified directory."""
    out_dir = tmp_path / "chunks"
    with patch(
        "sys.argv",
        [
            "split_files.py",
            str(sample_json_file),
            "--num-files",
            "3",
            "--output-dir",
            str(out_dir),
        ],
    ):
        main()
    chunks = sorted(out_dir.glob("split_file_*.json"))
    assert len(chunks) == 3
    assert sum(_line_count(str(c)) for c in chunks) == 25


def test_main_default_prefix(sample_json_file: Path, tmp_path: Path) -> None:
    """main() uses default prefix when --output-prefix is not given."""
    out_dir = tmp_path / "default_prefix"
    with patch(
        "sys.argv",
        [
            "split_files.py",
            str(sample_json_file),
            "--num-files",
            "2",
            "--output-dir",
            str(out_dir),
        ],
    ):
        main()
    assert (out_dir / "split_file_1.json").exists()
    assert (out_dir / "split_file_2.json").exists()
