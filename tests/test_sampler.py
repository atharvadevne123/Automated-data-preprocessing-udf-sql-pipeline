"""Tests for pipeline/sampler.py."""

from __future__ import annotations

import pytest

from pipeline.sampler import ReservoirSampler, StratifiedSampler


class TestReservoirSampler:
    def test_basic_sampling(self) -> None:
        sampler = ReservoirSampler(size=3)
        for i in range(10):
            sampler.add({"id": i})
        assert len(sampler.get_sample()) == 3

    def test_size_less_than_stream(self) -> None:
        sampler = ReservoirSampler(size=100)
        for i in range(5):
            sampler.add({"id": i})
        assert len(sampler.get_sample()) == 5

    def test_count_increments(self) -> None:
        sampler = ReservoirSampler(size=2)
        sampler.add({"a": 1})
        sampler.add({"a": 2})
        sampler.add({"a": 3})
        assert sampler.count == 3

    def test_reproducible_with_seed(self) -> None:
        records = [{"id": i} for i in range(20)]
        s1 = ReservoirSampler(size=5, seed=0)
        s2 = ReservoirSampler(size=5, seed=0)
        s1.add_batch(iter(records))
        s2.add_batch(iter(records))
        assert s1.get_sample() == s2.get_sample()

    def test_different_seeds_different_results(self) -> None:
        records = [{"id": i} for i in range(100)]
        s1 = ReservoirSampler(size=10, seed=1)
        s2 = ReservoirSampler(size=10, seed=2)
        s1.add_batch(iter(records))
        s2.add_batch(iter(records))
        assert s1.get_sample() != s2.get_sample()

    def test_invalid_size(self) -> None:
        with pytest.raises(ValueError):
            ReservoirSampler(size=0)

    def test_reset(self) -> None:
        sampler = ReservoirSampler(size=5)
        sampler.add({"x": 1})
        sampler.reset()
        assert sampler.count == 0
        assert sampler.get_sample() == []

    def test_get_sample_returns_copy(self) -> None:
        sampler = ReservoirSampler(size=3)
        sampler.add({"id": 1})
        s1 = sampler.get_sample()
        s1.append({"id": 99})
        assert len(sampler.get_sample()) == 1

    def test_add_batch(self) -> None:
        sampler = ReservoirSampler(size=5)
        sampler.add_batch(iter({"id": i} for i in range(10)))
        assert len(sampler.get_sample()) == 5


class TestStratifiedSampler:
    def _make_records(self) -> list[dict]:
        return [{"stars": (i % 5) + 1, "id": i} for i in range(50)]

    def test_basic_stratified(self) -> None:
        sampler = StratifiedSampler("stars", 0.5, seed=42)
        result = sampler.sample(self._make_records())
        assert 0 < len(result) < 50

    def test_full_sample_rate(self) -> None:
        sampler = StratifiedSampler("stars", 1.0)
        records = self._make_records()
        result = sampler.sample(records)
        assert len(result) == len(records)

    def test_invalid_rate_zero(self) -> None:
        with pytest.raises(ValueError):
            StratifiedSampler("stars", 0.0)

    def test_invalid_rate_above_one(self) -> None:
        with pytest.raises(ValueError):
            StratifiedSampler("stars", 1.5)

    def test_strata_counts(self) -> None:
        sampler = StratifiedSampler("stars", 0.1)
        records = [{"stars": 1}] * 10 + [{"stars": 2}] * 20
        counts = sampler.strata_counts(records)
        assert counts[1] == 10
        assert counts[2] == 20

    def test_missing_key_goes_to_none_stratum(self) -> None:
        sampler = StratifiedSampler("missing_key", 1.0)
        records = [{"a": 1}, {"a": 2}]
        result = sampler.sample(records)
        assert len(result) == 2

    @pytest.mark.parametrize("rate", [0.1, 0.5, 1.0])
    def test_various_rates(self, rate: float) -> None:
        sampler = StratifiedSampler("stars", rate, seed=0)
        records = [{"stars": i % 3} for i in range(30)]
        result = sampler.sample(records)
        assert len(result) > 0
