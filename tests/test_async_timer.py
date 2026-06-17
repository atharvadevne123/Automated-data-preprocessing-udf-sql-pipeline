"""Tests for AsyncTimer in utils/profiler.py and to_parquet_compatible in pipeline/exporter.py."""

from __future__ import annotations

import asyncio
import json

import pytest

from pipeline.exporter import DataExporter
from utils.profiler import AsyncTimer


class TestAsyncTimer:
    def test_measures_elapsed(self) -> None:
        async def _run() -> float:
            async with AsyncTimer("test") as t:
                await asyncio.sleep(0.01)
            return t.elapsed_sec

        elapsed = asyncio.run(_run())
        assert elapsed >= 0.005

    def test_label(self) -> None:
        async def _run() -> str:
            async with AsyncTimer("my_label") as t:
                pass
            return t.label

        assert asyncio.run(_run()) == "my_label"

    def test_to_dict(self) -> None:
        async def _run() -> dict:
            async with AsyncTimer("op") as t:
                pass
            return t.to_dict()

        d = asyncio.run(_run())
        assert d["label"] == "op"
        assert "elapsed_sec" in d
        assert d["elapsed_sec"] >= 0.0

    def test_initial_elapsed_zero(self) -> None:
        t = AsyncTimer("x")
        assert t.elapsed_sec == 0.0

    def test_no_sleep_very_fast(self) -> None:
        async def _run() -> float:
            async with AsyncTimer("fast") as t:
                pass
            return t.elapsed_sec

        elapsed = asyncio.run(_run())
        assert elapsed >= 0.0

    @pytest.mark.parametrize("label", ["fetch", "compute", ""])
    def test_various_labels(self, label: str) -> None:
        t = AsyncTimer(label)
        assert t.label == label


class TestToParquetCompatible:
    def test_flattens_nested_dict(self) -> None:
        exporter = DataExporter()
        records = [{"id": 1, "meta": {"k": "v"}}]
        result = exporter.to_parquet_compatible(records)
        assert isinstance(result[0]["meta"], str)
        parsed = json.loads(result[0]["meta"])
        assert parsed["k"] == "v"

    def test_flattens_list_field(self) -> None:
        exporter = DataExporter()
        records = [{"id": 1, "tags": ["a", "b"]}]
        result = exporter.to_parquet_compatible(records)
        assert isinstance(result[0]["tags"], str)
        assert json.loads(result[0]["tags"]) == ["a", "b"]

    def test_scalar_values_unchanged(self) -> None:
        exporter = DataExporter()
        records = [{"id": 1, "stars": 4.5, "text": "hi"}]
        result = exporter.to_parquet_compatible(records)
        assert result[0]["id"] == 1
        assert result[0]["stars"] == 4.5
        assert result[0]["text"] == "hi"

    def test_scalar_only_false_returns_unchanged(self) -> None:
        exporter = DataExporter()
        records = [{"meta": {"k": "v"}}]
        result = exporter.to_parquet_compatible(records, scalar_only=False)
        assert result[0]["meta"] == {"k": "v"}

    def test_empty_records(self) -> None:
        exporter = DataExporter()
        assert exporter.to_parquet_compatible([]) == []

    def test_original_not_mutated(self) -> None:
        exporter = DataExporter()
        records = [{"nested": {"a": 1}}]
        exporter.to_parquet_compatible(records)
        assert isinstance(records[0]["nested"], dict)
