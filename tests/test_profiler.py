"""Tests for utils/profiler.py."""

from __future__ import annotations

import time

import pytest

from utils.profiler import FunctionProfiler, profile_memory, timed


class TestFunctionProfiler:
    def test_call_count(self):
        fn = FunctionProfiler(lambda x: x + 1)
        fn(1)
        fn(2)
        fn(3)
        assert fn.stats["calls"] == 3

    def test_returns_correct_value(self):
        fn = FunctionProfiler(lambda x: x * 2)
        assert fn(5) == 10

    def test_total_sec_positive(self):
        fn = FunctionProfiler(lambda: time.sleep(0.01))
        fn()
        assert fn.stats["total_sec"] > 0

    def test_min_max_sec(self):
        fn = FunctionProfiler(lambda x: x)
        fn(1)
        fn(2)
        assert fn.stats["min_sec"] <= fn.stats["max_sec"]

    def test_avg_sec(self):
        fn = FunctionProfiler(lambda: None)
        fn()
        fn()
        assert fn.stats["calls"] == 2
        assert fn.stats["avg_sec"] > 0 or fn.stats["total_sec"] == 0

    def test_reset(self):
        fn = FunctionProfiler(lambda: None)
        fn()
        fn.reset()
        assert fn.stats["calls"] == 0
        assert fn.stats["total_sec"] == 0.0

    def test_zero_calls_avg(self):
        fn = FunctionProfiler(lambda: None)
        assert fn.stats["avg_sec"] == 0.0
        assert fn.stats["calls"] == 0

    @pytest.mark.parametrize("n", [1, 5, 10])
    def test_call_count_parametrized(self, n):
        fn = FunctionProfiler(lambda: None)
        for _ in range(n):
            fn()
        assert fn.stats["calls"] == n


class TestTimedContext:
    def test_elapsed_populated(self):
        with timed(log=False) as t:
            pass
        assert "elapsed_sec" in t
        assert t["elapsed_sec"] >= 0

    def test_elapsed_positive(self):
        with timed(log=False) as t:
            time.sleep(0.01)
        assert t["elapsed_sec"] > 0.005

    def test_label_does_not_error(self):
        with timed("my label", log=False) as t:
            pass
        assert t["elapsed_sec"] >= 0


class TestProfileMemory:
    def test_returns_dict_with_keys(self):
        result = profile_memory("test")
        assert "current_mb" in result
        assert "peak_mb" in result
        assert result["label"] == "test"

    def test_mb_values_non_negative(self):
        result = profile_memory()
        assert result["current_mb"] >= 0
        assert result["peak_mb"] >= 0
