"""Extended tests for utils/validators.py — additional edge cases."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.validators import (
    ValidationError,
    coerce_record,
    validate_jsonl_file,
    validate_num_files,
    validate_output_prefix,
)


def test_validate_jsonl_file_mixed_valid_invalid(tmp_path: Path) -> None:
    f = tmp_path / "mixed.jsonl"
    lines = [
        json.dumps({"id": 1}),
        "BROKEN",
        json.dumps({"id": 3}),
        "ALSO BROKEN",
        json.dumps({"id": 5}),
    ]
    f.write_text("\n".join(lines) + "\n")
    errors = validate_jsonl_file(f)
    assert len(errors) == 2
    assert any("Line 2" in e for e in errors)
    assert any("Line 4" in e for e in errors)


def test_validate_jsonl_file_all_blank_lines(tmp_path: Path) -> None:
    f = tmp_path / "blanks.jsonl"
    f.write_text("\n\n\n\n")
    errors = validate_jsonl_file(f)
    assert errors == []


def test_validate_num_files_exactly_one() -> None:
    assert validate_num_files(1) == 1


def test_validate_num_files_large() -> None:
    assert validate_num_files(10_000) == 10_000


def test_validate_output_prefix_with_slash_prefix(tmp_path: Path) -> None:
    prefix = str(tmp_path / "out_")
    result = validate_output_prefix(prefix)
    assert result == prefix


def test_coerce_record_with_nested_dict() -> None:
    rec = {"id": 1, "meta": {"city": "NYC", "state": "NY"}}
    result = coerce_record(rec, ["id", "meta"])
    assert result["meta"]["city"] == "NYC"


def test_coerce_record_with_list_value() -> None:
    rec = {"id": 1, "tags": ["a", "b", "c"]}
    result = coerce_record(rec, ["id", "tags"])
    assert result["tags"] == ["a", "b", "c"]


def test_coerce_record_with_none_value() -> None:
    rec = {"id": 1, "description": None}
    result = coerce_record(rec, ["id", "description"])
    assert result["description"] is None


@pytest.mark.parametrize(
    "bad_input",
    ["string", 42, 3.14, True, None, [1, 2, 3]],
)
def test_coerce_record_non_dict_raises(bad_input: object) -> None:
    with pytest.raises(ValidationError, match="Expected dict"):
        coerce_record(bad_input, ["id"])


def test_validate_jsonl_file_single_valid_record(tmp_path: Path) -> None:
    f = tmp_path / "one.jsonl"
    f.write_text(json.dumps({"id": 0}) + "\n")
    assert validate_jsonl_file(f) == []


def test_validate_jsonl_file_single_invalid_record(tmp_path: Path) -> None:
    f = tmp_path / "one_bad.jsonl"
    f.write_text("NOT JSON\n")
    errors = validate_jsonl_file(f)
    assert len(errors) == 1
    assert "Line 1" in errors[0]
