"""Tests for scripts/ — validate_jsonl CLI tool."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


def run_validate(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "scripts.validate_jsonl"] + args,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )


@pytest.fixture
def valid_jsonl(tmp_path: Path) -> Path:
    f = tmp_path / "valid.jsonl"
    with open(f, "w") as fh:
        for i in range(10):
            fh.write(json.dumps({"id": i}) + "\n")
    return f


@pytest.fixture
def invalid_jsonl(tmp_path: Path) -> Path:
    f = tmp_path / "invalid.jsonl"
    f.write_text('{"id": 1}\nNOT JSON\n{"id": 3}\n')
    return f


def test_validate_jsonl_valid_file_exits_0(valid_jsonl: Path) -> None:
    result = run_validate([str(valid_jsonl)])
    assert result.returncode == 0


def test_validate_jsonl_invalid_file_exits_1(invalid_jsonl: Path) -> None:
    result = run_validate([str(invalid_jsonl)])
    assert result.returncode == 1


def test_validate_jsonl_missing_file_exits_nonzero(tmp_path: Path) -> None:
    result = run_validate([str(tmp_path / "ghost.jsonl")])
    assert result.returncode != 0


def test_validate_jsonl_reports_errors(invalid_jsonl: Path) -> None:
    result = run_validate([str(invalid_jsonl)])
    assert "Line 2" in result.stderr or "error" in result.stderr.lower()


def test_validate_jsonl_quiet_flag_suppresses_success(valid_jsonl: Path) -> None:
    result = run_validate([str(valid_jsonl), "--quiet"])
    assert result.returncode == 0
    assert "PASSED" not in result.stderr


def test_validate_jsonl_max_errors_flag(tmp_path: Path) -> None:
    f = tmp_path / "many_errors.jsonl"
    with open(f, "w") as fh:
        for _ in range(20):
            fh.write("NOT JSON\n")
    result = run_validate([str(f), "--max-errors", "3"])
    assert result.returncode == 1
    error_lines = [l for l in result.stderr.splitlines() if "Line" in l]
    assert len(error_lines) <= 3


@pytest.mark.parametrize("n", [5, 10, 50])
def test_validate_jsonl_various_sizes(tmp_path: Path, n: int) -> None:
    f = tmp_path / f"data_{n}.jsonl"
    with open(f, "w") as fh:
        for i in range(n):
            fh.write(json.dumps({"id": i}) + "\n")
    result = run_validate([str(f)])
    assert result.returncode == 0
