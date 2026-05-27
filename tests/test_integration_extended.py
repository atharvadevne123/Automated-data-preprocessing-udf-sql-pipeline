"""Extended integration tests for the split_files CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


def run_cli(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "split_files.py"] + args,
        capture_output=True,
        text=True,
        cwd=cwd,
    )


PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture
def medium_jsonl(tmp_path: Path) -> Path:
    f = tmp_path / "medium.jsonl"
    with open(f, "w") as fh:
        for i in range(100):
            fh.write(json.dumps({"id": i, "v": i**2}) + "\n")
    return f


def test_cli_stdout_is_empty(medium_jsonl: Path, tmp_path: Path) -> None:
    result = run_cli(
        [str(medium_jsonl), "--num-files", "5", "--output-dir", str(tmp_path / "out")],
        PROJECT_ROOT,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == ""


def test_cli_stderr_contains_done(medium_jsonl: Path, tmp_path: Path) -> None:
    result = run_cli(
        [str(medium_jsonl), "--num-files", "5", "--output-dir", str(tmp_path / "out")],
        PROJECT_ROOT,
    )
    assert "Done" in result.stderr


def test_cli_zero_num_files_exits_nonzero(medium_jsonl: Path, tmp_path: Path) -> None:
    result = run_cli([str(medium_jsonl), "--num-files", "0"], PROJECT_ROOT)
    assert result.returncode != 0


def test_cli_negative_num_files_exits_nonzero(medium_jsonl: Path, tmp_path: Path) -> None:
    result = run_cli([str(medium_jsonl), "--num-files", "-5"], PROJECT_ROOT)
    assert result.returncode != 0


def test_cli_output_json_files_are_parseable(medium_jsonl: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "parse_check"
    result = run_cli(
        [str(medium_jsonl), "--num-files", "4", "--output-dir", str(out_dir)],
        PROJECT_ROOT,
    )
    assert result.returncode == 0
    for chunk in out_dir.glob("*.json"):
        with open(chunk, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    json.loads(line)  # must not raise


def test_cli_output_dir_nested_creation(medium_jsonl: Path, tmp_path: Path) -> None:
    deep = tmp_path / "a" / "b" / "c" / "d"
    result = run_cli(
        [str(medium_jsonl), "--num-files", "2", "--output-dir", str(deep)],
        PROJECT_ROOT,
    )
    assert result.returncode == 0
    assert deep.is_dir()


@pytest.mark.parametrize("prefix", ["r_", "data_", "review_", "yelp_chunk_"])
def test_cli_output_prefix_applied_correctly(medium_jsonl: Path, tmp_path: Path, prefix: str) -> None:
    out_dir = tmp_path / f"prefix_{prefix}"
    result = run_cli(
        [
            str(medium_jsonl),
            "--num-files",
            "3",
            "--output-dir",
            str(out_dir),
            "--output-prefix",
            prefix,
        ],
        PROJECT_ROOT,
    )
    assert result.returncode == 0
    files = list(out_dir.glob(f"{prefix}*.json"))
    assert len(files) == 3
