"""Extended data-quality tests verifying JSON schema preservation after split."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from split_files import split_file


def _read_all(paths: list[str]) -> list[dict]:
    records = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def test_stars_range_preserved_after_split(sample_jsonl: Path, tmp_path: Path) -> None:
    outputs = split_file(sample_jsonl, str(tmp_path / "s_"), 5)
    for rec in _read_all(outputs):
        assert 1 <= rec["stars"] <= 5


def test_string_fields_unchanged_after_split(tmp_path: Path) -> None:
    records = [{"id": i, "text": f"review-{'x' * 100}-{i}"} for i in range(10)]
    f = tmp_path / "strings.jsonl"
    _write_jsonl(f, records)
    outputs = split_file(f, str(tmp_path / "out_"), 3)
    result_records = _read_all(outputs)
    assert sorted(r["text"] for r in result_records) == sorted(r["text"] for r in records)


def test_numeric_fields_unchanged_after_split(tmp_path: Path) -> None:
    records = [{"id": i, "value": i * 3.14159} for i in range(15)]
    f = tmp_path / "floats.jsonl"
    _write_jsonl(f, records)
    outputs = split_file(f, str(tmp_path / "out_"), 3)
    result_values = sorted(r["value"] for r in _read_all(outputs))
    expected_values = sorted(r["value"] for r in records)
    assert result_values == pytest.approx(expected_values)


def test_nested_dict_preserved_after_split(tmp_path: Path) -> None:
    records = [
        {"id": i, "meta": {"city": f"City{i}", "rating": i % 5}}
        for i in range(12)
    ]
    f = tmp_path / "nested.jsonl"
    _write_jsonl(f, records)
    outputs = split_file(f, str(tmp_path / "out_"), 4)
    result_records = _read_all(outputs)
    assert all("meta" in r for r in result_records)
    assert all("city" in r["meta"] for r in result_records)


def test_list_fields_preserved_after_split(tmp_path: Path) -> None:
    records = [{"id": i, "tags": [f"tag{j}" for j in range(i % 4)]} for i in range(8)]
    f = tmp_path / "lists.jsonl"
    _write_jsonl(f, records)
    outputs = split_file(f, str(tmp_path / "out_"), 2)
    result_records = _read_all(outputs)
    assert all("tags" in r for r in result_records)


@pytest.mark.parametrize("null_field", ["description", "author", "category"])
def test_null_values_preserved_after_split(tmp_path: Path, null_field: str) -> None:
    records = [{"id": i, null_field: None} for i in range(6)]
    f = tmp_path / f"nulls_{null_field}.jsonl"
    _write_jsonl(f, records)
    outputs = split_file(f, str(tmp_path / "out_"), 2)
    result_records = _read_all(outputs)
    assert all(r[null_field] is None for r in result_records)
