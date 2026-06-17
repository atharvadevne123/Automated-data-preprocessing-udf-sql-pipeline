"""Stratified and reservoir sampling for Yelp pipeline records."""

from __future__ import annotations

import logging
import random
from typing import Any, Iterator

logger = logging.getLogger(__name__)


class ReservoirSampler:
    """Vitter's Algorithm R reservoir sampler for streaming records.

    Maintains a fixed-size reservoir of randomly chosen records from an
    arbitrary-length stream without knowing the stream length in advance.

    Args:
        size: Reservoir capacity (must be >= 1).
        seed: Optional random seed for reproducibility.

    Example::

        sampler = ReservoirSampler(size=100, seed=42)
        for record in stream:
            sampler.add(record)
        sample = sampler.get_sample()
    """

    def __init__(self, size: int, seed: int | None = None) -> None:
        if size < 1:
            raise ValueError(f"size must be >= 1, got {size}")
        self._size = size
        self._reservoir: list[dict[str, Any]] = []
        self._count = 0
        self._rng = random.Random(seed)

    def add(self, record: dict[str, Any]) -> None:
        """Add a single record to the reservoir.

        Args:
            record: Record dict to consider.
        """
        self._count += 1
        if len(self._reservoir) < self._size:
            self._reservoir.append(record)
        else:
            j = self._rng.randint(0, self._count - 1)
            if j < self._size:
                self._reservoir[j] = record

    def add_batch(self, records: Iterator[dict[str, Any]]) -> None:
        """Add all records from *records* to the reservoir.

        Args:
            records: Iterator of record dicts.
        """
        for record in records:
            self.add(record)

    def get_sample(self) -> list[dict[str, Any]]:
        """Return a copy of the current reservoir contents.

        Returns:
            List of up to *size* records.
        """
        return list(self._reservoir)

    @property
    def count(self) -> int:
        """Total records added so far."""
        return self._count

    def reset(self) -> None:
        """Clear reservoir and reset counter."""
        self._reservoir.clear()
        self._count = 0
        logger.info("ReservoirSampler reset (size=%d).", self._size)


class StratifiedSampler:
    """Sample a fixed fraction from each stratum defined by a key field.

    Useful for maintaining class balance when randomly sampling a dataset.

    Args:
        key_field: Record field whose value defines the stratum.
        sample_rate: Fraction of each stratum to retain, in (0, 1].
        seed: Optional random seed.

    Example::

        sampler = StratifiedSampler(key_field="stars", sample_rate=0.1, seed=0)
        result = sampler.sample(records)
    """

    def __init__(
        self,
        key_field: str,
        sample_rate: float,
        seed: int | None = None,
    ) -> None:
        if not (0.0 < sample_rate <= 1.0):
            raise ValueError(f"sample_rate must be in (0, 1], got {sample_rate}")
        self._key_field = key_field
        self._rate = sample_rate
        self._rng = random.Random(seed)

    def sample(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return a stratified sample from *records*.

        Args:
            records: Full list of records to sample from.

        Returns:
            Sampled subset preserving approximate stratum proportions.
        """
        strata: dict[Any, list[dict[str, Any]]] = {}
        for rec in records:
            key = rec.get(self._key_field)
            strata.setdefault(key, []).append(rec)

        result: list[dict[str, Any]] = []
        for key, group in strata.items():
            n = max(1, round(len(group) * self._rate))
            chosen = self._rng.sample(group, min(n, len(group)))
            result.extend(chosen)
            logger.debug("Stratum %s: %d/%d sampled.", key, len(chosen), len(group))

        logger.info(
            "StratifiedSampler: %d/%d records sampled across %d strata.",
            len(result),
            len(records),
            len(strata),
        )
        return result

    def strata_counts(self, records: list[dict[str, Any]]) -> dict[Any, int]:
        """Return the count of records per stratum without sampling.

        Args:
            records: Records to analyse.

        Returns:
            Mapping of stratum key to record count.
        """
        counts: dict[Any, int] = {}
        for rec in records:
            key = rec.get(self._key_field)
            counts[key] = counts.get(key, 0) + 1
        return counts
