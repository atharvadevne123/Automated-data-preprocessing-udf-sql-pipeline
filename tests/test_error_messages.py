"""Tests for quality and clarity of error messages."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from split_files import split_file
from utils.validators import (
    ValidationError,
    validate_input_path,
    validate_jsonl_file,
    validate_num_files,
    validate_output_prefix,
)


def test_split_file_not_found_message_contains_path(tmp_path: Path) -> None:
    path = tmp_path / "nonexistent.json"
    with pytest.raises(FileNotFoundError, match=str(path)):
        split_file(path, str(tmp_path / "out_"), 5)


def test_split_file_empty_message_contains_filename(tmp_path: Path) -> None:
    f = tmp_path / "empty_reviews.json"
    f.write_text("")
    with pytest.raises(ValueError, match="empty"):
        split_file(f, str(tmp_path / "out_"), 3)


def test_split_file_invalid_num_files_message_shows_value(tmp_path: Path) -> None:
    f = tmp_path / "data.json"
    f.write_text('{"id": 0}\n')
    with pytest.raises(ValueError, match="-3"):
        split_file(f, str(tmp_path / "out_"), -3)


def test_validate_input_path_missing_shows_path(tmp_path: Path) -> None:
    path = tmp_path / "missing_file.jsonl"
    with pytest.raises(ValidationError, match=str(path.name)):
        validate_input_path(path)


def test_validate_num_files_message_shows_value() -> None:
    with pytest.raises(ValidationError, match="-7"):
        validate_num_files(-7)


def test_validate_output_prefix_empty_message() -> None:
    with pytest.raises(ValidationError, match="must not be empty"):
        validate_output_prefix("")


def test_validate_jsonl_file_missing_shows_filename(tmp_path: Path) -> None:
    path = tmp_path / "ghost.jsonl"
    with pytest.raises(ValidationError, match="ghost.jsonl"):
        validate_jsonl_file(path)


@pytest.mark.parametrize("bad_n", [0, -1, -999])
def test_validate_num_files_always_shows_bad_value(bad_n: int) -> None:
    with pytest.raises(ValidationError, match=str(bad_n)):
        validate_num_files(bad_n)
