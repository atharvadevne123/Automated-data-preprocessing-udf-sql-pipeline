"""Tests for utils/validators.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.validators import (
    ValidationError,
    coerce_record,
    validate_input_path,
    validate_jsonl_file,
    validate_num_files,
    validate_output_prefix,
)


# ---------- validate_input_path ----------


def test_validate_input_path_existing_file(tmp_path: Path) -> None:
    f = tmp_path / "data.json"
    f.write_text('{"id": 1}\n')
    result = validate_input_path(f)
    assert result == f.resolve()


def test_validate_input_path_missing(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="does not exist"):
        validate_input_path(tmp_path / "ghost.json")


def test_validate_input_path_directory(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="not a regular file"):
        validate_input_path(tmp_path)


# ---------- validate_num_files ----------


@pytest.mark.parametrize("n", [1, 2, 10, 100, 1000])
def test_validate_num_files_valid(n: int) -> None:
    assert validate_num_files(n) == n


@pytest.mark.parametrize("n", [0, -1, -100])
def test_validate_num_files_invalid(n: int) -> None:
    with pytest.raises(ValidationError, match="num_files must be"):
        validate_num_files(n)


# ---------- validate_jsonl_file ----------


def test_validate_jsonl_file_valid(tmp_path: Path) -> None:
    f = tmp_path / "ok.jsonl"
    with open(f, "w") as fh:
        for i in range(5):
            fh.write(json.dumps({"id": i}) + "\n")
    errors = validate_jsonl_file(f)
    assert errors == []


def test_validate_jsonl_file_detects_bad_json(tmp_path: Path) -> None:
    f = tmp_path / "bad.jsonl"
    f.write_text('{"id": 1}\nNOT JSON\n{"id": 3}\n')
    errors = validate_jsonl_file(f)
    assert len(errors) == 1
    assert "Line 2" in errors[0]


def test_validate_jsonl_file_missing(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="not found"):
        validate_jsonl_file(tmp_path / "missing.jsonl")


def test_validate_jsonl_file_respects_max_errors(tmp_path: Path) -> None:
    f = tmp_path / "many_bad.jsonl"
    with open(f, "w") as fh:
        for _ in range(20):
            fh.write("NOT JSON\n")
    errors = validate_jsonl_file(f, max_errors=5)
    assert len(errors) == 5


def test_validate_jsonl_file_empty(tmp_path: Path) -> None:
    f = tmp_path / "empty.jsonl"
    f.write_text("")
    errors = validate_jsonl_file(f)
    assert errors == []


# ---------- validate_output_prefix ----------


@pytest.mark.parametrize("prefix", ["out_", "chunk-", "part.", "x", "my_prefix_"])
def test_validate_output_prefix_valid(prefix: str) -> None:
    assert validate_output_prefix(prefix) == prefix


def test_validate_output_prefix_empty_raises() -> None:
    with pytest.raises(ValidationError, match="must not be empty"):
        validate_output_prefix("")


# ---------- coerce_record ----------


def test_coerce_record_valid() -> None:
    rec = {"id": 1, "text": "hello"}
    result = coerce_record(rec, ["id", "text"])
    assert result is rec


def test_coerce_record_not_a_dict() -> None:
    with pytest.raises(ValidationError, match="Expected dict"):
        coerce_record([1, 2, 3], ["id"])


def test_coerce_record_missing_field() -> None:
    with pytest.raises(ValidationError, match="missing required fields"):
        coerce_record({"id": 1}, ["id", "name"])


def test_coerce_record_extra_fields_allowed() -> None:
    rec = {"id": 1, "name": "Alice", "extra": True}
    result = coerce_record(rec, ["id", "name"])
    assert result["extra"] is True
