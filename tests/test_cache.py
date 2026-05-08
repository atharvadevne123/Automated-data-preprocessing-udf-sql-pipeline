"""Tests for utils.cache.LRUCache and cached decorator."""

from __future__ import annotations

import pytest

from utils.cache import CacheStats, LRUCache, cached


class TestCacheStats:
    def test_initial_state(self):
        s = CacheStats()
        assert s.hits == 0
        assert s.misses == 0
        assert s.evictions == 0

    def test_total(self):
        s = CacheStats()
        s.hits = 3
        s.misses = 2
        assert s.total == 5

    def test_hit_rate_zero_when_no_lookups(self):
        assert CacheStats().hit_rate == 0.0

    def test_hit_rate_calculated(self):
        s = CacheStats()
        s.hits = 8
        s.misses = 2
        assert s.hit_rate == pytest.approx(0.8)

    def test_to_dict_has_keys(self):
        s = CacheStats()
        d = s.to_dict()
        assert "hits" in d
        assert "misses" in d
        assert "hit_rate" in d

    def test_reset_clears_all(self):
        s = CacheStats()
        s.hits = 5
        s.misses = 3
        s.reset()
        assert s.hits == 0
        assert s.misses == 0
        assert s.total == 0


class TestLRUCache:
    def _make(self, calls):
        def func(x):
            calls.append(x)
            return x * 2

        return LRUCache(func, maxsize=10)

    def test_returns_correct_result(self):
        calls = []
        c = self._make(calls)
        assert c(5) == 10

    def test_first_call_is_miss(self):
        calls = []
        c = self._make(calls)
        c(5)
        assert c.stats.misses == 1
        assert c.stats.hits == 0

    def test_second_call_is_hit(self):
        calls = []
        c = self._make(calls)
        c(5)
        c(5)
        assert c.stats.hits == 1

    def test_different_args_both_miss(self):
        calls = []
        c = self._make(calls)
        c(1)
        c(2)
        assert c.stats.misses == 2
        assert len(calls) == 2

    def test_cache_clear_resets_stats(self):
        calls = []
        c = self._make(calls)
        c(1)
        c(1)
        c.cache_clear()
        assert c.stats.hits == 0
        assert c.stats.misses == 0

    def test_cache_clear_invalidates_cached_values(self):
        calls = []
        c = self._make(calls)
        c(1)
        c.cache_clear()
        c(1)
        assert len(calls) == 2

    def test_cache_info_delegates(self):
        calls = []
        c = self._make(calls)
        c(1)
        info = c.cache_info()
        assert hasattr(info, "hits")
        assert hasattr(info, "misses")


class TestCachedDecorator:
    def test_decorates_function(self):
        calls = []

        @cached(maxsize=10)
        def square(x):
            calls.append(x)
            return x * x

        assert square(4) == 16

    def test_caches_result(self):
        calls = []

        @cached(maxsize=10)
        def square(x):
            calls.append(x)
            return x * x

        square(4)
        square(4)
        assert len(calls) == 1

    def test_has_cache_attribute(self):
        @cached(maxsize=10)
        def func(x):
            return x

        assert hasattr(func, "cache")
        assert isinstance(func.cache, LRUCache)

    def test_stats_accessible_via_cache(self):
        @cached(maxsize=10)
        def func(x):
            return x

        func(1)
        func(1)
        assert func.cache.stats.hits >= 1

    def test_preserves_function_name(self):
        @cached(maxsize=10)
        def my_func(x):
            return x

        assert my_func.__name__ == "my_func"

    @pytest.mark.parametrize("n", [1, 5, 10])
    def test_hit_rate_improves_with_repeated_calls(self, n):
        @cached(maxsize=100)
        def func(x):
            return x

        for _ in range(n):
            func(99)
        if n > 1:
            assert func.cache.stats.hit_rate > 0
