"""Tests for concurrent/parallel usage safety of split_file."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from split_files import split_file


def _write_jsonl(path: Path, n: int) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(json.dumps({"id": i, "value": i * 2}) + "\n")


def _line_count(path: str | Path) -> int:
    return sum(1 for line in open(path, encoding="utf-8") if line.strip())


def test_split_thread_safe_separate_files(tmp_path: Path) -> None:
    """Two threads splitting different files should not interfere."""
    file_a = tmp_path / "a.jsonl"
    file_b = tmp_path / "b.jsonl"
    _write_jsonl(file_a, 30)
    _write_jsonl(file_b, 50)

    results: dict[str, list[str]] = {}
    errors: list[Exception] = []

    def run_split(key: str, input_file: Path, prefix: str, n: int) -> None:
        try:
            results[key] = split_file(input_file, prefix, n)
        except Exception as exc:
            errors.append(exc)

    t1 = threading.Thread(target=run_split, args=("a", file_a, str(tmp_path / "a_out_"), 3))
    t2 = threading.Thread(target=run_split, args=("b", file_b, str(tmp_path / "b_out_"), 5))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors, f"Thread errors: {errors}"
    assert sum(_line_count(o) for o in results["a"]) == 30
    assert sum(_line_count(o) for o in results["b"]) == 50


def test_split_produces_correct_output_after_repeated_calls(tmp_path: Path) -> None:
    f = tmp_path / "data.jsonl"
    _write_jsonl(f, 20)
    for run in range(3):
        out_dir = tmp_path / f"run_{run}"
        out_dir.mkdir()
        outputs = split_file(f, str(out_dir / "chunk_"), 4)
        assert sum(_line_count(o) for o in outputs) == 20


@pytest.mark.parametrize("n", [10, 25, 50])
def test_split_idempotent_output(tmp_path: Path, n: int) -> None:
    """Splitting the same file twice produces identical line counts."""
    f = tmp_path / f"idem_{n}.jsonl"
    _write_jsonl(f, n)

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    out1.mkdir()
    out2.mkdir()

    outputs1 = split_file(f, str(out1 / "c_"), 5)
    outputs2 = split_file(f, str(out2 / "c_"), 5)

    counts1 = [_line_count(o) for o in outputs1]
    counts2 = [_line_count(o) for o in outputs2]
    assert counts1 == counts2
