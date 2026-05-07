"""Tests for split_files.py version flag and get_file_size_mb helper."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from split_files import __version__, get_file_size_mb, main


def test_version_flag_exits_0(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "split_files.py", "--version"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0
    assert __version__ in result.stdout or __version__ in result.stderr


def test_version_string_format() -> None:
    assert __version__.count(".") >= 1


def test_get_file_size_mb_existing_file(tmp_path: Path) -> None:
    f = tmp_path / "test.json"
    f.write_bytes(b"x" * 1024 * 1024)  # exactly 1 MB
    size = get_file_size_mb(f)
    assert size == pytest.approx(1.0, abs=0.001)


def test_get_file_size_mb_nonexistent_returns_zero(tmp_path: Path) -> None:
    size = get_file_size_mb(tmp_path / "ghost.json")
    assert size == 0.0


def test_get_file_size_mb_empty_file(tmp_path: Path) -> None:
    f = tmp_path / "empty.json"
    f.write_text("")
    assert get_file_size_mb(f) == 0.0


@pytest.mark.parametrize("bytes_,expected_mb", [
    (0, 0.0),
    (512 * 1024, 0.5),
    (2 * 1024 * 1024, 2.0),
])
def test_get_file_size_mb_various_sizes(
    tmp_path: Path, bytes_: int, expected_mb: float
) -> None:
    f = tmp_path / "sized.bin"
    f.write_bytes(b"x" * bytes_)
    assert get_file_size_mb(f) == pytest.approx(expected_mb, abs=0.001)


def test_main_version_via_argv() -> None:
    with patch("sys.argv", ["split_files.py", "--version"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
    assert exc_info.value.code == 0
