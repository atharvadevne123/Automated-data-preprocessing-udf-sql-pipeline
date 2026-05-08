"""Extended tests for DataExporter."""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path

import pytest

from pipeline.exporter import DataExporter


@pytest.fixture
def exporter():
    return DataExporter()


@pytest.fixture
def records():
    return [{"id": i, "name": f"item_{i}", "value": i * 1.5} for i in range(20)]


class TestJsonlRoundTrip:
    def test_round_trip_preserves_data(self, exporter, records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "rt.jsonl"
            exporter.to_jsonl(records, path)
            loaded = [json.loads(l) for l in path.read_text().splitlines()]
            assert loaded == records

    def test_unicode_preserved(self, exporter):
        records = [{"text": "café résumé naïve 日本語"}]
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "unicode.jsonl"
            exporter.to_jsonl(records, path)
            loaded = json.loads(path.read_text().splitlines()[0])
            assert loaded["text"] == records[0]["text"]

    def test_special_chars_in_values(self, exporter):
        records = [{"text": 'line1\nline2\ttab"quote'}]
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "special.jsonl"
            count = exporter.to_jsonl(records, path)
            assert count == 1


class TestCsvEdgeCases:
    def test_extra_fields_excluded(self, exporter):
        records = [{"a": 1, "b": 2, "c": 3}]
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.csv"
            exporter.to_csv(records, path, fields=["a", "b"])
            with open(path) as f:
                header = f.readline().strip()
            assert "c" not in header

    def test_csv_row_count_matches(self, exporter, records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.csv"
            count = exporter.to_csv(records, path)
            with open(path) as f:
                rows = list(csv.DictReader(f))
            assert len(rows) == count == len(records)

    def test_csv_values_correct(self, exporter):
        records = [{"name": "Alice", "score": 99}]
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.csv"
            exporter.to_csv(records, path)
            with open(path) as f:
                row = list(csv.DictReader(f))[0]
            assert row["name"] == "Alice"
            assert int(row["score"]) == 99


class TestJsonArrayEdgeCases:
    def test_indent_default_two(self, exporter, records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.json"
            exporter.to_json(records[:2], path)
            content = path.read_text()
            assert "  " in content

    def test_empty_json_array(self, exporter):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "empty.json"
            count = exporter.to_json([], path)
            assert count == 0
            assert json.loads(path.read_text()) == []


class TestExportDispatchEdgeCases:
    @pytest.mark.parametrize("fmt_variant", ["JSONL", "Jsonl", "JSON", "CSV"])
    def test_case_insensitive_format(self, exporter, records, fmt_variant):
        with tempfile.TemporaryDirectory() as d:
            fmt = fmt_variant.lower()
            path = Path(d) / f"out.{fmt}"
            kwargs = {"fields": ["id"]} if fmt == "csv" else {}
            count = exporter.export(records, path, fmt=fmt_variant, **kwargs)
            assert count == len(records)

    def test_dot_prefix_stripped(self, exporter, records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.jsonl"
            count = exporter.export(records, path, fmt=".jsonl")
            assert count == len(records)

    @pytest.mark.parametrize("n", [1, 5, 50, 200])
    def test_large_batches(self, exporter, n):
        recs = [{"id": i} for i in range(n)]
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.jsonl"
            count = exporter.to_jsonl(recs, path)
            assert count == n
