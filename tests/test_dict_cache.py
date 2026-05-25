"""Tests for DictCache with TTL in utils/cache.py."""

from __future__ import annotations

import time

import pytest

from utils.cache import DictCache


class TestDictCache:
    def test_set_and_get(self):
        cache = DictCache()
        cache.set("key", "value")
        assert cache.get("key") == "value"

    def test_missing_key_returns_default(self):
        cache = DictCache()
        assert cache.get("missing") is None
        assert cache.get("missing", "fallback") == "fallback"

    def test_delete(self):
        cache = DictCache()
        cache.set("k", "v")
        cache.delete("k")
        assert cache.get("k") is None

    def test_clear(self):
        cache = DictCache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert len(cache) == 0

    def test_len(self):
        cache = DictCache()
        cache.set("x", 1)
        cache.set("y", 2)
        assert len(cache) == 2

    def test_ttl_expires(self):
        cache = DictCache(ttl=0.05)
        cache.set("k", "v")
        time.sleep(0.1)
        assert cache.get("k") is None

    def test_ttl_not_expired(self):
        cache = DictCache(ttl=5.0)
        cache.set("k", "v")
        assert cache.get("k") == "v"

    def test_no_ttl_never_expires(self):
        cache = DictCache(ttl=None)
        cache.set("k", "v")
        assert cache.get("k") == "v"

    def test_maxsize_evicts_oldest(self):
        cache = DictCache(maxsize=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        assert len(cache) == 2
        assert cache.get("a") is None

    def test_maxsize_no_evict_on_update(self):
        cache = DictCache(maxsize=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("a", 99)
        assert len(cache) == 2
        assert cache.get("a") == 99

    def test_delete_nonexistent(self):
        cache = DictCache()
        cache.delete("nope")

    @pytest.mark.parametrize("key,val", [("a", 1), ("b", "str"), ("c", [1, 2])])
    def test_parametrized_types(self, key, val):
        cache = DictCache()
        cache.set(key, val)
        assert cache.get(key) == val
