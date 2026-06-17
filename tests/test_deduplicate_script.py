"""Tests for scripts/deduplicate.py CLI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.deduplicate import main, parse_args


class TestParseArgs:
    def test_required_args(self) -> None:
        args = parse_args(["input.jsonl", "output.jsonl"])
        assert args.input == Path("input.jsonl")
        assert args.output == Path("output.jsonl")

    def test_key_fields(self) -> None:
        args = parse_args(["in.jsonl", "out.jsonl", "--key-fields", "id", "name"])
        assert args.key_fields == ["id", "name"]

    def test_stats_flag(self) -> None:
        args = parse_args(["in.jsonl", "out.jsonl", "--stats"])
        assert args.stats is True

    def test_defaults(self) -> None:
        args = parse_args(["in.jsonl", "out.jsonl"])
        assert args.key_fields is None
        assert args.stats is False


class TestMain:
    def _write_input(self, tmp_path: Path, records: list[dict]) -> Path:
        f = tmp_path / "input.jsonl"
        f.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        return f

    def test_basic_dedup(self, tmp_path: Path) -> None:
        records = [{"id": 1, "v": "a"}, {"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
        inp = self._write_input(tmp_path, records)
        out = tmp_path / "output.jsonl"
        code = main([str(inp), str(out)])
        assert code == 0
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_with_key_fields(self, tmp_path: Path) -> None:
        records = [
            {"id": 1, "noise": "a"},
            {"id": 1, "noise": "b"},
            {"id": 2, "noise": "c"},
        ]
        inp = self._write_input(tmp_path, records)
        out = tmp_path / "output.jsonl"
        code = main([str(inp), str(out), "--key-fields", "id"])
        assert code == 0
        lines = [l for l in out.read_text().strip().split("\n") if l]
        assert len(lines) == 2

    def test_missing_input_returns_1(self, tmp_path: Path) -> None:
        code = main([str(tmp_path / "nonexistent.jsonl"), str(tmp_path / "out.jsonl")])
        assert code == 1

    def test_creates_output_dir(self, tmp_path: Path) -> None:
        records = [{"x": 1}]
        inp = self._write_input(tmp_path, records)
        out = tmp_path / "subdir" / "output.jsonl"
        code = main([str(inp), str(out)])
        assert code == 0
        assert out.exists()

    def test_stats_flag_runs(self, tmp_path: Path) -> None:
        records = [{"id": 1}, {"id": 1}]
        inp = self._write_input(tmp_path, records)
        out = tmp_path / "out.jsonl"
        code = main([str(inp), str(out), "--stats"])
        assert code == 0
