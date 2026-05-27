"""Simple LRU cache wrapper with hit/miss statistics."""

from __future__ import annotations

import functools
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


class CacheStats:
    """Tracks cache hits, misses, and evictions.

    Attributes:
        hits: Number of cache hits.
        misses: Number of cache misses.
        evictions: Number of evictions (LRU removals).
    """

    def __init__(self) -> None:
        self.hits: int = 0
        self.misses: int = 0
        self.evictions: int = 0

    @property
    def total(self) -> int:
        """Total cache lookups."""
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        """Cache hit rate as a fraction in [0.0, 1.0]."""
        return self.hits / self.total if self.total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "total": self.total,
            "hit_rate": round(self.hit_rate, 4),
        }

    def reset(self) -> None:
        """Reset all counters to zero."""
        self.hits = 0
        self.misses = 0
        self.evictions = 0


class LRUCache:
    """A bounded LRU cache backed by functools.lru_cache semantics.

    Wraps a user-supplied callable with an LRU cache and exposes hit/miss
    statistics via a :class:`CacheStats` instance.

    Args:
        func: The callable to cache.
        maxsize: Maximum number of entries; None means unlimited.

    Example::

        def expensive(x: int) -> int:
            return x * x

        cache = LRUCache(expensive, maxsize=128)
        result = cache(5)     # miss
        result = cache(5)     # hit
        print(cache.stats.hit_rate)
    """

    def __init__(self, func: Callable, maxsize: int | None = 128) -> None:
        self._stats = CacheStats()
        self._maxsize = maxsize
        self._cached = functools.lru_cache(maxsize=maxsize)(func)
        functools.update_wrapper(self, func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        info_before = self._cached.cache_info()
        result = self._cached(*args, **kwargs)
        info_after = self._cached.cache_info()
        if info_after.hits > info_before.hits:
            self._stats.hits += 1
        else:
            self._stats.misses += 1
        if info_after.currsize < info_before.currsize + (1 if info_after.misses > info_before.misses else 0):
            self._stats.evictions += max(0, info_before.currsize - info_after.currsize)
        return result

    @property
    def stats(self) -> CacheStats:
        """Return live cache statistics."""
        return self._stats

    def cache_info(self) -> Any:
        """Delegate to the underlying lru_cache cache_info()."""
        return self._cached.cache_info()

    def cache_clear(self) -> None:
        """Clear the underlying cache and reset statistics."""
        self._cached.cache_clear()
        self._stats.reset()
        logger.info("LRUCache cleared for %s", getattr(self._cached, "__name__", "?"))


class DictCache:
    """Simple time-to-live (TTL) dict cache for arbitrary keys.

    Entries expire after *ttl* seconds from insertion.  A TTL of ``None``
    means entries never expire.

    Args:
        ttl: Seconds before an entry expires; None for no expiry.
        maxsize: Maximum number of entries; oldest are evicted when exceeded.

    Example::

        cache = DictCache(ttl=60)
        cache.set("key", "value")
        val = cache.get("key")   # returns "value"
    """

    def __init__(self, ttl: float | None = None, maxsize: int | None = None) -> None:
        import time as _time

        self._ttl = ttl
        self._maxsize = maxsize
        self._store: dict[Any, tuple[Any, float]] = {}
        self._time = _time

    def set(self, key: Any, value: Any) -> None:
        """Insert *key* → *value* into the cache."""
        if self._maxsize is not None and len(self._store) >= self._maxsize and key not in self._store:
            oldest = next(iter(self._store))
            del self._store[oldest]
        self._store[key] = (value, self._time.monotonic())

    def get(self, key: Any, default: Any = None) -> Any:
        """Return the cached value for *key*, or *default* if missing/expired."""
        if key not in self._store:
            return default
        value, ts = self._store[key]
        if self._ttl is not None and (self._time.monotonic() - ts) > self._ttl:
            del self._store[key]
            return default
        return value

    def delete(self, key: Any) -> None:
        """Remove *key* from the cache if present."""
        self._store.pop(key, None)

    def clear(self) -> None:
        """Clear all entries."""
        self._store.clear()

    def __len__(self) -> int:
        return len(self._store)

    def __contains__(self, key: Any) -> bool:
        return self.get(key, _SENTINEL := object()) is not _SENTINEL


def cached(maxsize: int | None = 128) -> Callable:
    """Decorator factory that wraps a function in an :class:`LRUCache`.

    The wrapped function gains a ``.cache`` attribute of type :class:`LRUCache`
    for accessing hit/miss statistics.

    Args:
        maxsize: Maximum LRU cache size.

    Returns:
        Decorator that wraps the target function.

    Example::

        @cached(maxsize=256)
        def parse_record(raw: str) -> dict:
            return json.loads(raw)

        parse_record('{"a":1}')
        print(parse_record.cache.stats.hit_rate)
    """

    def decorator(func: Callable) -> Callable:
        lru = LRUCache(func, maxsize=maxsize)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return lru(*args, **kwargs)

        wrapper.cache = lru  # type: ignore[attr-defined]
        return wrapper

    return decorator
