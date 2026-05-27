"""Extended tests for utils/metrics.py."""

from __future__ import annotations

import pytest

from utils.metrics import SplitMetrics, Timer


def test_split_metrics_to_dict_rounded_elapsed() -> None:
    m = SplitMetrics(input_file="f.jsonl", elapsed_sec=1.23456789)
    d = m.to_dict()
    assert d["elapsed_sec"] == 1.2346  # rounded to 4 decimal places


def test_split_metrics_to_dict_rounded_lines_per_sec() -> None:
    m = SplitMetrics(input_file="f.jsonl", total_lines=333, elapsed_sec=1.0)
    d = m.to_dict()
    assert d["lines_per_second"] == 333.0


def test_split_metrics_chunk_sizes_sum() -> None:
    m = SplitMetrics(input_file="f.jsonl", chunk_sizes=[10, 10, 11])
    assert sum(m.chunk_sizes) == 31


def test_split_metrics_input_file_preserved() -> None:
    path = "/data/input/reviews.jsonl"
    m = SplitMetrics(input_file=path)
    assert m.to_dict()["input_file"] == path


@pytest.mark.parametrize(
    "total,elapsed,expected_rate",
    [
        (1000, 1.0, 1000.0),
        (500, 2.0, 250.0),
        (0, 1.0, 0.0),
        (100, 0.5, 200.0),
    ],
)
def test_split_metrics_lines_per_second_parametrized(total: int, elapsed: float, expected_rate: float) -> None:
    m = SplitMetrics(input_file="f.jsonl", total_lines=total, elapsed_sec=elapsed)
    assert m.lines_per_second == pytest.approx(expected_rate)


def test_timer_elapsed_increases() -> None:
    import time

    with Timer() as t:
        time.sleep(0.02)
    assert t.elapsed >= 0.015


def test_timer_multiple_uses() -> None:
    t = Timer()
    with t:
        pass
    with t:
        pass
    second = t.elapsed
    assert second >= 0  # timer resets on each __enter__


def test_split_metrics_equality_not_implemented() -> None:
    a = SplitMetrics(input_file="a.jsonl", total_lines=10)
    b = SplitMetrics(input_file="b.jsonl", total_lines=10)
    assert a != b  # different input_file


def test_split_metrics_to_dict_mb_per_second_present() -> None:
    m = SplitMetrics(input_file="x.jsonl", bytes_read=2 * 1024 * 1024, elapsed_sec=2.0)
    d = m.to_dict()
    assert d["mb_per_second"] == pytest.approx(1.0, abs=0.01)
