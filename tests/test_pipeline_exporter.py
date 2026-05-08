"""Tests for pipeline.exporter.DataExporter."""

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
def sample_records():
    return [
        {"id": 1, "stars": 4, "text": "great"},
        {"id": 2, "stars": 2, "text": "bad"},
        {"id": 3, "stars": 5, "text": "amazing"},
    ]


class TestToJsonl:
    def test_writes_correct_number_of_lines(self, exporter, sample_records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.jsonl"
            count = exporter.to_jsonl(sample_records, path)
            assert count == 3

    def test_output_is_valid_jsonl(self, exporter, sample_records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.jsonl"
            exporter.to_jsonl(sample_records, path)
            lines = path.read_text().strip().splitlines()
            parsed = [json.loads(line) for line in lines]
            assert len(parsed) == 3

    def test_creates_parent_directory(self, exporter, sample_records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "nested" / "dir" / "out.jsonl"
            exporter.to_jsonl(sample_records, path)
            assert path.exists()

    def test_empty_records(self, exporter):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "empty.jsonl"
            count = exporter.to_jsonl([], path)
            assert count == 0


class TestToJson:
    def test_writes_json_array(self, exporter, sample_records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.json"
            count = exporter.to_json(sample_records, path)
            data = json.loads(path.read_text())
            assert isinstance(data, list)
            assert len(data) == count == 3

    def test_custom_indent(self, exporter, sample_records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.json"
            exporter.to_json(sample_records, path, indent=4)
            content = path.read_text()
            assert "    " in content


class TestToCsv:
    def test_writes_csv_with_header(self, exporter, sample_records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.csv"
            count = exporter.to_csv(sample_records, path)
            with open(path) as f:
                reader = list(csv.DictReader(f))
            assert len(reader) == 3
            assert count == 3

    def test_custom_fields(self, exporter, sample_records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.csv"
            exporter.to_csv(sample_records, path, fields=["id", "stars"])
            with open(path) as f:
                header = f.readline().strip()
            assert header == "id,stars"

    def test_empty_records_no_fields_raises(self, exporter):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.csv"
            with pytest.raises(ValueError):
                exporter.to_csv([], path)

    def test_empty_records_with_fields(self, exporter):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.csv"
            count = exporter.to_csv([], path, fields=["id", "stars"])
            assert count == 0


class TestExportDispatch:
    @pytest.mark.parametrize("fmt", ["jsonl", "json", "csv"])
    def test_dispatches_all_formats(self, exporter, sample_records, fmt):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / f"out.{fmt}"
            kwargs = {"fields": ["id", "stars"]} if fmt == "csv" else {}
            count = exporter.export(sample_records, path, fmt=fmt, **kwargs)
            assert count == 3

    def test_unknown_format_raises(self, exporter, sample_records):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.xyz"
            with pytest.raises(ValueError, match="Unsupported export format"):
                exporter.export(sample_records, path, fmt="xyz")
