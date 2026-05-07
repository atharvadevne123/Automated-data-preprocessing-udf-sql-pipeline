"""Tests for utils/metrics.py."""

from __future__ import annotations

import time

import pytest

from utils.metrics import SplitMetrics, Timer

# ---------- SplitMetrics ----------


def test_split_metrics_defaults() -> None:
    m = SplitMetrics(input_file="data.jsonl")
    assert m.total_lines == 0
    assert m.num_chunks == 0
    assert m.elapsed_sec == 0.0
    assert m.bytes_read == 0
    assert m.chunk_sizes == []


def test_lines_per_second_with_elapsed() -> None:
    m = SplitMetrics(input_file="data.jsonl", total_lines=1000, elapsed_sec=2.0)
    assert m.lines_per_second == pytest.approx(500.0)


def test_lines_per_second_zero_elapsed() -> None:
    m = SplitMetrics(input_file="data.jsonl", total_lines=100, elapsed_sec=0.0)
    assert m.lines_per_second == 0.0


def test_mb_per_second_with_data() -> None:
    m = SplitMetrics(input_file="f", bytes_read=1024 * 1024, elapsed_sec=1.0)
    assert m.mb_per_second == pytest.approx(1.0)


def test_mb_per_second_zero_elapsed() -> None:
    m = SplitMetrics(input_file="f", bytes_read=1000, elapsed_sec=0.0)
    assert m.mb_per_second == 0.0


def test_to_dict_keys() -> None:
    m = SplitMetrics(
        input_file="test.jsonl",
        total_lines=100,
        num_chunks=5,
        elapsed_sec=0.5,
        bytes_read=2048,
        chunk_sizes=[20, 20, 20, 20, 20],
    )
    d = m.to_dict()
    assert d["input_file"] == "test.jsonl"
    assert d["total_lines"] == 100
    assert d["num_chunks"] == 5
    assert d["chunk_sizes"] == [20, 20, 20, 20, 20]
    assert "lines_per_second" in d
    assert "mb_per_second" in d


def test_to_dict_is_json_serialisable() -> None:
    import json

    m = SplitMetrics(input_file="x.jsonl", total_lines=50, elapsed_sec=0.1)
    json.dumps(m.to_dict())  # must not raise


# ---------- Timer ----------


def test_timer_measures_elapsed() -> None:
    with Timer() as t:
        time.sleep(0.05)
    assert t.elapsed >= 0.04


def test_timer_elapsed_is_zero_before_use() -> None:
    t = Timer()
    assert t.elapsed == 0.0


def test_timer_context_manager_returns_self() -> None:
    t = Timer()
    with t as ctx:
        assert ctx is t


def test_timer_survives_exception() -> None:
    t = Timer()
    try:
        with t:
            raise ValueError("oops")
    except ValueError:
        pass
    assert t.elapsed > 0
