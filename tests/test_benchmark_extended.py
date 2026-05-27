"""Extended tests for scripts/benchmark.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.benchmark import benchmark_split, generate_synthetic_jsonl


def test_generate_synthetic_jsonl_all_records_valid_json(tmp_path: Path) -> None:
    f = tmp_path / "synth.jsonl"
    generate_synthetic_jsonl(f, 20)
    with open(f, encoding="utf-8") as fh:
        for line in fh:
            obj = json.loads(line)
            assert isinstance(obj["id"], int)
            assert isinstance(obj["text"], str)
            assert 1 <= obj["stars"] <= 5


def test_benchmark_split_elapsed_is_positive(tmp_path: Path) -> None:
    input_file = tmp_path / "in.jsonl"
    generate_synthetic_jsonl(input_file, 50)
    metrics = benchmark_split(input_file, 5, tmp_path / "out")
    assert metrics["elapsed_sec"] >= 0


def test_benchmark_split_records_per_sec_positive(tmp_path: Path) -> None:
    input_file = tmp_path / "in.jsonl"
    generate_synthetic_jsonl(input_file, 100)
    metrics = benchmark_split(input_file, 10, tmp_path / "out")
    assert metrics["records_per_sec"] > 0


def test_benchmark_split_mb_per_sec_nonnegative(tmp_path: Path) -> None:
    input_file = tmp_path / "in.jsonl"
    generate_synthetic_jsonl(input_file, 50)
    metrics = benchmark_split(input_file, 5, tmp_path / "out")
    assert metrics["mb_per_sec"] >= 0


@pytest.mark.parametrize("records,chunks", [(10, 1), (30, 3), (100, 10)])
def test_benchmark_split_chunk_count_correct(tmp_path: Path, records: int, chunks: int) -> None:
    input_file = tmp_path / f"in_{records}.jsonl"
    generate_synthetic_jsonl(input_file, records)
    metrics = benchmark_split(input_file, chunks, tmp_path / f"out_{records}")
    assert metrics["num_chunks"] == chunks
    assert metrics["total_records"] == records


def test_generate_synthetic_jsonl_overrides_existing_file(tmp_path: Path) -> None:
    f = tmp_path / "overwrite.jsonl"
    generate_synthetic_jsonl(f, 5)
    generate_synthetic_jsonl(f, 10)
    count = sum(1 for _ in open(f, encoding="utf-8"))
    assert count == 10
