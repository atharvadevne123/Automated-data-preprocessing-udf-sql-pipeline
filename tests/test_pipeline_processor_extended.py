"""Extended tests for RecordProcessor covering complex filter/transform chains."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from pipeline.processor import RecordProcessor


def _make_jsonl(records: list[dict]) -> Path:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    for r in records:
        f.write(json.dumps(r) + "\n")
    f.flush()
    return Path(f.name)


class TestComplexFilterChains:
    def test_chained_filters_all_required(self):
        p = RecordProcessor(filters=[
            lambda r: r.get("stars", 0) >= 3,
            lambda r: len(r.get("text", "")) >= 5,
            lambda r: r.get("useful", 0) >= 1,
        ])
        records = [
            {"stars": 4, "text": "great", "useful": 2},
            {"stars": 4, "text": "great", "useful": 0},
            {"stars": 2, "text": "great", "useful": 2},
            {"stars": 4, "text": "hi", "useful": 2},
        ]
        result = list(p.process_records(iter(records)))
        assert len(result) == 1

    @pytest.mark.parametrize("stars_min,expected", [
        (1, 5), (2, 4), (3, 3), (4, 2), (5, 1), (6, 0)
    ])
    def test_star_filter_counts(self, stars_min, expected):
        records = [{"stars": i} for i in range(1, 6)]
        p = RecordProcessor(filters=[lambda r, m=stars_min: r["stars"] >= m])
        result = list(p.process_records(iter(records)))
        assert len(result) == expected


class TestTransformChains:
    def test_add_metadata_transform(self):
        import datetime
        p = RecordProcessor(transforms=[
            lambda r: {**r, "processed_at": "2026-01-01"},
            lambda r: {**r, "word_count": len(r.get("text", "").split())},
        ])
        record = {"text": "great food here"}
        result = p.process_record(record)
        assert result["processed_at"] == "2026-01-01"
        assert result["word_count"] == 3

    def test_normalise_stars_transform(self):
        p = RecordProcessor(transforms=[lambda r: {**r, "stars_norm": (r["stars"] - 1) / 4}])
        result = p.process_record({"stars": 5})
        assert result["stars_norm"] == pytest.approx(1.0)

    def test_transforms_see_previous_output(self):
        p = RecordProcessor(transforms=[
            lambda r: {**r, "x": 10},
            lambda r: {**r, "y": r["x"] * 2},
        ])
        result = p.process_record({})
        assert result["x"] == 10
        assert result["y"] == 20


class TestSamplingReproducibility:
    def test_same_seed_same_output(self):
        records = [{"id": i} for i in range(1000)]
        p1 = RecordProcessor(sample_rate=0.3, seed=12345)
        p2 = RecordProcessor(sample_rate=0.3, seed=12345)
        r1 = [r["id"] for r in p1.process_records(iter(records))]
        r2 = [r["id"] for r in p2.process_records(iter(records[:]))]
        assert r1 == r2

    def test_different_seeds_likely_different(self):
        records = [{"id": i} for i in range(200)]
        p1 = RecordProcessor(sample_rate=0.5, seed=1)
        p2 = RecordProcessor(sample_rate=0.5, seed=9999)
        r1 = set(r["id"] for r in p1.process_records(iter(records)))
        r2 = set(r["id"] for r in p2.process_records(iter(records[:])))
        assert r1 != r2


class TestProcessFileEdgeCases:
    def test_blank_lines_skipped(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"id": 1}\n')
            f.write('\n')
            f.write('{"id": 2}\n')
            path = Path(f.name)
        p = RecordProcessor()
        result = list(p.process_file(path))
        assert len(result) == 2

    def test_large_file_processes_all(self):
        records = [{"id": i, "stars": i % 5 + 1} for i in range(500)]
        path = _make_jsonl(records)
        p = RecordProcessor()
        result = list(p.process_file(path))
        assert len(result) == 500

    @pytest.mark.parametrize("n_records", [0, 1, 10, 100])
    def test_process_file_sizes(self, n_records):
        if n_records == 0:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
                path = Path(f.name)
        else:
            path = _make_jsonl([{"id": i} for i in range(n_records)])
        p = RecordProcessor()
        result = list(p.process_file(path))
        assert len(result) == n_records
