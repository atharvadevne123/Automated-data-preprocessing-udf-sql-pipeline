"""Extended tests for LRUCache and cached decorator."""

from __future__ import annotations

import pytest

from utils.cache import CacheStats, LRUCache, cached


class TestCacheStatsExtended:
    @pytest.mark.parametrize("hits,misses,expected_rate", [
        (0, 0, 0.0),
        (10, 0, 1.0),
        (0, 10, 0.0),
        (7, 3, 0.7),
        (1, 99, 0.01),
    ])
    def test_hit_rate_parametrized(self, hits, misses, expected_rate):
        s = CacheStats()
        s.hits = hits
        s.misses = misses
        assert s.hit_rate == pytest.approx(expected_rate)

    def test_to_dict_hit_rate_rounded(self):
        s = CacheStats()
        s.hits = 1
        s.misses = 3
        d = s.to_dict()
        assert d["hit_rate"] == pytest.approx(0.25)

    def test_evictions_in_dict(self):
        s = CacheStats()
        s.evictions = 5
        assert s.to_dict()["evictions"] == 5

    def test_reset_all_fields(self):
        s = CacheStats()
        s.hits = 100
        s.misses = 50
        s.evictions = 10
        s.reset()
        assert s.total == 0
        assert s.evictions == 0


class TestLRUCacheExtended:
    def test_maxsize_enforced(self):
        calls = []

        def f(x):
            calls.append(x)
            return x

        c = LRUCache(f, maxsize=2)
        for i in range(5):
            c(i)
        info = c.cache_info()
        assert info.maxsize == 2

    def test_none_maxsize_unlimited(self):
        c = LRUCache(lambda x: x, maxsize=None)
        info = c.cache_info()
        assert info.maxsize is None

    def test_cumulative_hits_after_multiple_calls(self):
        calls = []
        c = LRUCache(lambda x: calls.append(x) or x, maxsize=128)
        for _ in range(5):
            c(42)
        assert c.stats.hits == 4
        assert c.stats.misses == 1

    @pytest.mark.parametrize("n_calls", [1, 5, 10, 50])
    def test_cache_clear_after_n_calls(self, n_calls):
        c = LRUCache(lambda x: x, maxsize=128)
        for i in range(n_calls):
            c(i)
        c.cache_clear()
        assert c.stats.hits == 0
        assert c.stats.misses == 0
        assert c.cache_info().currsize == 0


class TestCachedDecoratorExtended:
    def test_multiple_different_functions(self):
        @cached(maxsize=10)
        def square(x):
            return x * x

        @cached(maxsize=10)
        def cube(x):
            return x ** 3

        assert square(4) == 16
        assert cube(3) == 27
        assert square.cache is not cube.cache

    def test_unhashable_args_raise_type_error(self):
        @cached(maxsize=10)
        def func(x):
            return x

        with pytest.raises(TypeError):
            func([1, 2, 3])

    def test_cache_cleared_by_cache_object(self):
        calls = []

        @cached(maxsize=10)
        def func(x):
            calls.append(x)
            return x

        func(1)
        func(1)
        func.cache.cache_clear()
        func(1)
        assert len(calls) == 2

    @pytest.mark.parametrize("maxsize", [1, 10, 128, None])
    def test_various_maxsizes(self, maxsize):
        @cached(maxsize=maxsize)
        def f(x):
            return x

        f(1)
        f(2)
        info = f.cache.cache_info()
        assert info.maxsize == maxsize
