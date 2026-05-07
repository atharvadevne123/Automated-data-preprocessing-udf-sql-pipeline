"""Tests that verify conftest.py fixtures produce correct data."""

from __future__ import annotations

import json
from pathlib import Path


def _read_records(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_sample_jsonl_fixture_has_25_records(sample_jsonl: Path) -> None:
    records = _read_records(sample_jsonl)
    assert len(records) == 25


def test_sample_jsonl_fixture_has_required_fields(sample_jsonl: Path) -> None:
    records = _read_records(sample_jsonl)
    for rec in records:
        assert "id" in rec
        assert "text" in rec
        assert "stars" in rec


def test_tiny_jsonl_fixture_has_3_records(tiny_jsonl: Path) -> None:
    records = _read_records(tiny_jsonl)
    assert len(records) == 3


def test_tiny_jsonl_fixture_has_id_and_value(tiny_jsonl: Path) -> None:
    records = _read_records(tiny_jsonl)
    for rec in records:
        assert "id" in rec
        assert "value" in rec


def test_single_line_jsonl_has_one_record(single_line_jsonl: Path) -> None:
    records = _read_records(single_line_jsonl)
    assert len(records) == 1
    assert records[0]["text"] == "only record"


def test_large_jsonl_fixture_has_100_records(large_jsonl: Path) -> None:
    records = _read_records(large_jsonl)
    assert len(records) == 100


def test_full_env_fixture_has_all_required_keys(full_env: dict) -> None:
    required = {
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    }
    assert required.issubset(full_env.keys())


def test_full_env_fixture_values_are_strings(full_env: dict) -> None:
    for val in full_env.values():
        assert isinstance(val, str)
