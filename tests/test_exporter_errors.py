"""Tests for DataExporter error handling."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipeline.exporter import DataExporter


class TestExporterErrorHandling:
    def test_to_jsonl_oserror_reraises(self, tmp_path):
        exporter = DataExporter()
        records = [{"a": 1}]
        bad_path = tmp_path / "nonexistent_dir" / "deeply" / "nested" / "file.jsonl"
        # Patch open to raise OSError
        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                exporter.to_jsonl(records, bad_path)

    def test_to_json_oserror_reraises(self, tmp_path):
        exporter = DataExporter()
        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                exporter.to_json([{"a": 1}], tmp_path / "out.json")

    def test_to_csv_oserror_reraises(self, tmp_path):
        exporter = DataExporter()
        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                exporter.to_csv([{"a": 1}], tmp_path / "out.csv")

    def test_to_csv_empty_no_fields_raises(self, tmp_path):
        exporter = DataExporter()
        with pytest.raises(ValueError, match="Cannot infer"):
            exporter.to_csv([], tmp_path / "out.csv", fields=None)

    def test_to_csv_empty_with_fields_ok(self, tmp_path):
        exporter = DataExporter()
        out = tmp_path / "out.csv"
        count = exporter.to_csv([], out, fields=["a", "b"])
        assert count == 0
        assert out.exists()

    def test_export_unsupported_format(self, tmp_path):
        exporter = DataExporter()
        with pytest.raises(ValueError, match="Unsupported export format"):
            exporter.export([{"a": 1}], tmp_path / "out.parquet", fmt="parquet")

    def test_to_jsonl_success_returns_count(self, tmp_path):
        exporter = DataExporter()
        records = [{"x": 1}, {"x": 2}, {"x": 3}]
        out = tmp_path / "out.jsonl"
        count = exporter.to_jsonl(records, out)
        assert count == 3
        assert out.exists()

    def test_to_json_creates_valid_json(self, tmp_path):
        import json
        exporter = DataExporter()
        records = [{"a": 1, "b": "hello"}]
        out = tmp_path / "out.json"
        exporter.to_json(records, out)
        loaded = json.loads(out.read_text())
        assert loaded == records

    @pytest.mark.parametrize("fmt,kwargs", [
        ("jsonl", {}),
        ("json", {}),
        ("csv", {"fields": ["k"]}),
    ])
    def test_export_dispatch_parametrized(self, tmp_path, fmt, kwargs):
        exporter = DataExporter()
        records = [{"k": "v"}]
        out = tmp_path / f"out.{fmt}"
        count = exporter.export(records, out, fmt=fmt, **kwargs)
        assert count == 1
        assert out.exists()
