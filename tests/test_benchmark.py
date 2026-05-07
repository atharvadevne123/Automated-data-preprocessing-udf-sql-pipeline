"""Tests for scripts/benchmark.py helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.benchmark import benchmark_split, generate_synthetic_jsonl


def test_generate_synthetic_jsonl_creates_file(tmp_path: Path) -> None:
    f = tmp_path / "synthetic.jsonl"
    generate_synthetic_jsonl(f, 100)
    assert f.exists()
    assert f.stat().st_size > 0


def test_generate_synthetic_jsonl_record_count(tmp_path: Path) -> None:
    f = tmp_path / "synthetic.jsonl"
    generate_synthetic_jsonl(f, 50)
    count = sum(1 for _ in open(f, encoding="utf-8"))
    assert count == 50


def test_generate_synthetic_jsonl_valid_json(tmp_path: Path) -> None:
    f = tmp_path / "synthetic.jsonl"
    generate_synthetic_jsonl(f, 10)
    with open(f, encoding="utf-8") as fh:
        for line in fh:
            obj = json.loads(line)
            assert "id" in obj
            assert "text" in obj
            assert "stars" in obj


def test_benchmark_split_returns_metrics(tmp_path: Path) -> None:
    input_file = tmp_path / "input.jsonl"
    generate_synthetic_jsonl(input_file, 100)
    output_dir = tmp_path / "chunks"
    metrics = benchmark_split(input_file, 5, output_dir)
    assert "elapsed_sec" in metrics
    assert "records_per_sec" in metrics
    assert "mb_per_sec" in metrics
    assert "total_records" in metrics
    assert metrics["total_records"] == 100
    assert metrics["num_chunks"] == 5


def test_benchmark_split_creates_chunks(tmp_path: Path) -> None:
    input_file = tmp_path / "input.jsonl"
    generate_synthetic_jsonl(input_file, 30)
    output_dir = tmp_path / "out"
    benchmark_split(input_file, 3, output_dir)
    chunks = list(output_dir.glob("*.json"))
    assert len(chunks) == 3


@pytest.mark.parametrize("n,chunks", [(10, 2), (50, 5), (100, 10)])
def test_benchmark_split_various_sizes(tmp_path: Path, n: int, chunks: int) -> None:
    input_file = tmp_path / f"input_{n}.jsonl"
    generate_synthetic_jsonl(input_file, n)
    output_dir = tmp_path / f"out_{n}"
    metrics = benchmark_split(input_file, chunks, output_dir)
    assert metrics["total_records"] == n
    assert metrics["elapsed_sec"] >= 0
