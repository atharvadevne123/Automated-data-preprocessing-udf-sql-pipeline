"""Integration tests for the end-to-end dedup → partition → export flow."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from pipeline.deduplicator import RecordDeduplicator
from pipeline.exporter import DataExporter
from pipeline.partitioner import FilePartitioner
from pipeline.sampler import ReservoirSampler


def _make_records(n: int = 20) -> list[dict]:
    records = []
    for i in range(n):
        records.append({
            "review_id": f"r{i:04d}",
            "business_id": f"biz{i % 5}",
            "stars": (i % 5) + 1,
            "text": f"Review number {i} about this place",
        })
    # Add duplicates
    records += records[:3]
    return records


class TestDeduplicateToExport:
    def test_dedup_reduces_count(self) -> None:
        records = _make_records(10)
        dedup = RecordDeduplicator(key_fields=["review_id"], track_stats=True)
        unique = list(dedup.deduplicate(records))
        assert len(unique) == 10
        assert dedup.stats.duplicates_dropped == 3

    def test_dedup_then_export_jsonl(self, tmp_path: Path) -> None:
        records = _make_records(10)
        dedup = RecordDeduplicator(key_fields=["review_id"])
        unique = list(dedup.deduplicate(records))
        out = tmp_path / "output.jsonl"
        exporter = DataExporter()
        count = exporter.to_jsonl(unique, out)
        assert count == 10
        assert out.exists()
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 10

    def test_reservoir_sample_then_parquet_compatible(self) -> None:
        records = _make_records(20)
        rs = ReservoirSampler(size=5, seed=99)
        rs.add_batch(records)
        sample = rs.get_sample()
        assert len(sample) == 5
        exporter = DataExporter()
        flat = exporter.to_parquet_compatible(sample)
        assert len(flat) == 5
        for rec in flat:
            assert "stars" in rec

    def test_partition_then_count_files(self, tmp_path: Path) -> None:
        records = _make_records(20)
        dedup = RecordDeduplicator(key_fields=["review_id"])
        unique = list(dedup.deduplicate(records))
        fp = FilePartitioner(
            key_func=lambda r: str(int(r["stars"])),
            output_dir=tmp_path,
            prefix="stars",
        )
        fp.add_batch(unique)
        fp.flush()
        out_files = list(tmp_path.glob("stars_*.jsonl"))
        assert len(out_files) == 5  # stars 1-5

    def test_full_flow_preserves_content(self, tmp_path: Path) -> None:
        records = _make_records(5)
        dedup = RecordDeduplicator(key_fields=["review_id"])
        unique = list(dedup.deduplicate(records))
        out = tmp_path / "result.json"
        exporter = DataExporter()
        exporter.to_json(unique, out)
        loaded = json.loads(out.read_text())
        assert len(loaded) == 5
        assert all("review_id" in r for r in loaded)

    def test_to_parquet_compatible_nested(self) -> None:
        records = [{"id": 1, "meta": {"a": 1}, "tags": ["x", "y"]}]
        exporter = DataExporter()
        flat = exporter.to_parquet_compatible(records)
        assert isinstance(flat[0]["meta"], str)
        assert isinstance(flat[0]["tags"], str)
        assert json.loads(flat[0]["meta"]) == {"a": 1}
