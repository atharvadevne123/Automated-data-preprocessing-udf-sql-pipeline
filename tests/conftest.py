"""Shared pytest fixtures for the test suite."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def sample_jsonl(tmp_path: Path) -> Path:
    """25-record newline-delimited JSON file."""
    filepath = tmp_path / "sample.jsonl"
    with open(filepath, "w", encoding="utf-8") as f:
        for i in range(25):
            f.write(json.dumps({"id": i, "text": f"review {i}", "stars": i % 5 + 1}) + "\n")
    return filepath


@pytest.fixture
def tiny_jsonl(tmp_path: Path) -> Path:
    """3-record newline-delimited JSON file."""
    filepath = tmp_path / "tiny.jsonl"
    with open(filepath, "w", encoding="utf-8") as f:
        for i in range(3):
            f.write(json.dumps({"id": i, "value": i * 10}) + "\n")
    return filepath


@pytest.fixture
def single_line_jsonl(tmp_path: Path) -> Path:
    """Single-record JSON file."""
    filepath = tmp_path / "single.jsonl"
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(json.dumps({"id": 0, "text": "only record"}) + "\n")
    return filepath


@pytest.fixture
def large_jsonl(tmp_path: Path) -> Path:
    """100-record JSON file for performance-related tests."""
    filepath = tmp_path / "large.jsonl"
    with open(filepath, "w", encoding="utf-8") as f:
        for i in range(100):
            f.write(
                json.dumps(
                    {
                        "id": i,
                        "text": f"This is review number {i} with some longer text",
                        "stars": i % 5 + 1,
                        "useful": i % 3,
                    }
                )
                + "\n"
            )
    return filepath


@pytest.fixture
def full_env() -> dict[str, str]:
    """Full set of Snowflake environment variables."""
    return {
        "SNOWFLAKE_ACCOUNT": "test-account",
        "SNOWFLAKE_USER": "test-user",
        "SNOWFLAKE_PASSWORD": "test-password",
        "SNOWFLAKE_WAREHOUSE": "test-wh",
        "SNOWFLAKE_DATABASE": "test-db",
        "SNOWFLAKE_SCHEMA": "PUBLIC",
    }
