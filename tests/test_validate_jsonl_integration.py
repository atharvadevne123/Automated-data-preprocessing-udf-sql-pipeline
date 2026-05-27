"""Integration tests: validate_jsonl.py used after split_file output."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.validate_jsonl import main as validate_main
from split_files import split_file
from utils.validators import validate_jsonl_file


@pytest.fixture
def split_output(tmp_path: Path, sample_jsonl: Path) -> list[str]:
    return split_file(sample_jsonl, str(tmp_path / "chunk_"), 5)


def test_all_split_chunks_pass_validation(split_output: list[str]) -> None:
    for path in split_output:
        errors = validate_jsonl_file(Path(path))
        assert errors == [], f"Chunk {path} has errors: {errors}"


def test_validate_main_on_split_chunk_returns_0(split_output: list[str]) -> None:
    for path in split_output:
        exit_code = validate_main([path])
        assert exit_code == 0, f"validate_main returned {exit_code} for {path}"


def test_corrupted_chunk_detected_after_split(split_output: list[str], tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.json"
    with open(corrupt, "w") as f:
        f.write('{"id": 1}\nNOT JSON\n{"id": 3}\n')
    errors = validate_jsonl_file(corrupt)
    assert len(errors) > 0


def test_empty_split_output_passes_validation(tmp_path: Path) -> None:
    """A split chunk that has only valid records should pass."""
    f = tmp_path / "valid.jsonl"
    f.write_text(json.dumps({"id": 0, "ok": True}) + "\n")
    errors = validate_jsonl_file(f)
    assert errors == []


@pytest.mark.parametrize("num_chunks", [2, 5, 10])
def test_pipeline_split_then_validate(sample_jsonl: Path, tmp_path: Path, num_chunks: int) -> None:
    outputs = split_file(sample_jsonl, str(tmp_path / f"n{num_chunks}_"), num_chunks)
    for path in outputs:
        errors = validate_jsonl_file(Path(path))
        assert errors == [], f"Chunk {path} failed: {errors}"
