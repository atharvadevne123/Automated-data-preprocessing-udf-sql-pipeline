"""Tests for pipeline.processor.RecordProcessor."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from pipeline.processor import RecordProcessor


def _records(n: int = 5) -> list[dict]:
    return [{"id": i, "stars": i % 5 + 1, "text": f"review {i}"} for i in range(n)]


class TestRecordProcessorInit:
    def test_default_init(self):
        p = RecordProcessor()
        assert p._sample_rate == 1.0

    def test_invalid_sample_rate_zero(self):
        with pytest.raises(ValueError):
            RecordProcessor(sample_rate=0.0)

    def test_invalid_sample_rate_negative(self):
        with pytest.raises(ValueError):
            RecordProcessor(sample_rate=-0.1)

    def test_invalid_sample_rate_above_one(self):
        with pytest.raises(ValueError):
            RecordProcessor(sample_rate=1.1)


class TestRecordProcessorFilters:
    def test_no_filters_passes_all(self):
        p = RecordProcessor()
        result = p.process_record({"stars": 3})
        assert result is not None

    def test_single_filter_keep(self):
        p = RecordProcessor(filters=[lambda r: r["stars"] >= 4])
        assert p.process_record({"stars": 5}) is not None

    def test_single_filter_drop(self):
        p = RecordProcessor(filters=[lambda r: r["stars"] >= 4])
        assert p.process_record({"stars": 2}) is None

    def test_multiple_filters_all_must_pass(self):
        p = RecordProcessor(
            filters=[lambda r: r["stars"] >= 3, lambda r: r.get("useful", 0) > 0]
        )
        assert p.process_record({"stars": 4, "useful": 1}) is not None
        assert p.process_record({"stars": 4, "useful": 0}) is None

    def test_filter_returns_empty_list_when_all_dropped(self):
        p = RecordProcessor(filters=[lambda r: False])
        result = list(p.process_records(iter(_records())))
        assert result == []


class TestRecordProcessorTransforms:
    def test_transform_applied(self):
        p = RecordProcessor(transforms=[lambda r: {**r, "flag": True}])
        result = p.process_record({"stars": 3})
        assert result["flag"] is True

    def test_multiple_transforms_applied_in_order(self):
        p = RecordProcessor(
            transforms=[
                lambda r: {**r, "a": 1},
                lambda r: {**r, "b": r["a"] + 1},
            ]
        )
        result = p.process_record({})
        assert result["a"] == 1
        assert result["b"] == 2


class TestRecordProcessorSampling:
    def test_full_sample_keeps_all(self):
        p = RecordProcessor(sample_rate=1.0)
        result = list(p.process_records(iter(_records(100))))
        assert len(result) == 100

    def test_half_sample_keeps_roughly_half(self):
        p = RecordProcessor(sample_rate=0.5, seed=42)
        result = list(p.process_records(iter(_records(1000))))
        assert 400 < len(result) < 600


class TestRecordProcessorFile:
    def test_process_file_reads_jsonl(self):
        records = _records(5)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
            path = Path(f.name)
        p = RecordProcessor()
        result = list(p.process_file(path))
        assert len(result) == 5

    def test_process_file_not_found(self):
        p = RecordProcessor()
        with pytest.raises(FileNotFoundError):
            list(p.process_file(Path("/nonexistent/file.jsonl")))

    def test_process_file_skips_invalid_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"stars": 3}\n')
            f.write("NOT JSON\n")
            f.write('{"stars": 5}\n')
            path = Path(f.name)
        p = RecordProcessor()
        result = list(p.process_file(path))
        assert len(result) == 2

    def test_process_file_to_list(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for i in range(3):
                f.write(json.dumps({"id": i}) + "\n")
            path = Path(f.name)
        p = RecordProcessor()
        result = p.process_file_to_list(path)
        assert isinstance(result, list)
        assert len(result) == 3

    @pytest.mark.parametrize("n", [0, 1, 10, 50])
    def test_process_records_various_sizes(self, n):
        p = RecordProcessor()
        result = list(p.process_records(iter(_records(n))))
        assert len(result) == n
