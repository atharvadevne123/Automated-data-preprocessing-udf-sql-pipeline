"""Integration tests: full pipeline from CLI invocation to validated output."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


def _line_count(path: str | Path) -> int:
    return sum(1 for line in open(path, encoding="utf-8") if line.strip())


def _read_records(path: str | Path) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


@pytest.fixture
def cli_input(tmp_path: Path) -> Path:
    filepath = tmp_path / "input.json"
    with open(filepath, "w", encoding="utf-8") as f:
        for i in range(20):
            f.write(json.dumps({"id": i, "msg": f"hello {i}"}) + "\n")
    return filepath


def run_cli(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "split_files.py"] + args,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )


def test_cli_basic_split(cli_input: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    result = run_cli([str(cli_input), "--num-files", "4", "--output-dir", str(out_dir)])
    assert result.returncode == 0, result.stderr
    chunks = list(out_dir.glob("*.json"))
    assert len(chunks) == 4
    total = sum(_line_count(c) for c in chunks)
    assert total == 20


def test_cli_preserves_data_integrity(cli_input: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "integrity"
    result = run_cli([str(cli_input), "--num-files", "5", "--output-dir", str(out_dir)])
    assert result.returncode == 0
    all_records = []
    for chunk in sorted(out_dir.glob("*.json")):
        all_records.extend(_read_records(chunk))
    ids = sorted(rec["id"] for rec in all_records)
    assert ids == list(range(20))


def test_cli_missing_file_exits_nonzero(tmp_path: Path) -> None:
    result = run_cli([str(tmp_path / "nonexistent.json"), "--num-files", "3"])
    assert result.returncode != 0


def test_cli_creates_output_dir(cli_input: Path, tmp_path: Path) -> None:
    new_dir = tmp_path / "brand" / "new" / "dir"
    assert not new_dir.exists()
    result = run_cli([str(cli_input), "--num-files", "2", "--output-dir", str(new_dir)])
    assert result.returncode == 0
    assert new_dir.exists()


def test_cli_custom_prefix(cli_input: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "prefix_test"
    result = run_cli(
        [
            str(cli_input),
            "--num-files",
            "3",
            "--output-dir",
            str(out_dir),
            "--output-prefix",
            "chunk_",
        ]
    )
    assert result.returncode == 0
    assert (out_dir / "chunk_1.json").exists()
    assert (out_dir / "chunk_2.json").exists()
    assert (out_dir / "chunk_3.json").exists()


def test_cli_single_file_output(cli_input: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "single"
    result = run_cli([str(cli_input), "--num-files", "1", "--output-dir", str(out_dir)])
    assert result.returncode == 0
    chunks = list(out_dir.glob("*.json"))
    assert len(chunks) == 1
    assert _line_count(chunks[0]) == 20


def test_cli_logs_to_stderr(cli_input: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "log_test"
    result = run_cli([str(cli_input), "--num-files", "2", "--output-dir", str(out_dir)])
    assert result.returncode == 0
    assert "Done" in result.stderr or "Written" in result.stderr


@pytest.mark.parametrize("n", [2, 4, 5, 10, 20])
def test_cli_various_chunk_counts(cli_input: Path, tmp_path: Path, n: int) -> None:
    out_dir = tmp_path / f"n{n}"
    result = run_cli([str(cli_input), "--num-files", str(n), "--output-dir", str(out_dir)])
    assert result.returncode == 0
    total = sum(_line_count(c) for c in out_dir.glob("*.json"))
    assert total == 20
