"""Tests for scripts/partition_data.py CLI."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.partition_data import main, parse_args


class TestParseArgs:
    def test_required_args(self) -> None:
        args = parse_args(["input.jsonl", "/tmp/out"])
        assert args.input == Path("input.jsonl")
        assert args.output_dir == Path("/tmp/out")

    def test_field_default(self) -> None:
        args = parse_args(["in.jsonl", "/out"])
        assert args.field == "stars"

    def test_custom_field(self) -> None:
        args = parse_args(["in.jsonl", "/out", "--field", "category"])
        assert args.field == "category"

    def test_prefix_default(self) -> None:
        args = parse_args(["in.jsonl", "/out"])
        assert args.prefix == "partition"

    def test_custom_prefix(self) -> None:
        args = parse_args(["in.jsonl", "/out", "--prefix", "stars"])
        assert args.prefix == "stars"


class TestMain:
    def _write_input(self, tmp_path: Path, records: list[dict]) -> Path:
        f = tmp_path / "input.jsonl"
        f.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        return f

    def test_creates_partition_files(self, tmp_path: Path) -> None:
        records = [{"stars": 5}, {"stars": 4}, {"stars": 5}]
        inp = self._write_input(tmp_path, records)
        out = tmp_path / "parts"
        code = main([str(inp), str(out)])
        assert code == 0
        files = list(out.glob("*.jsonl"))
        assert len(files) == 2

    def test_correct_content_in_partitions(self, tmp_path: Path) -> None:
        records = [{"stars": 5, "id": 1}, {"stars": 4, "id": 2}]
        inp = self._write_input(tmp_path, records)
        out = tmp_path / "parts"
        code = main([str(inp), str(out), "--field", "stars"])
        assert code == 0
        for f in out.glob("*.jsonl"):
            lines = [json.loads(l) for l in f.read_text().strip().split("\n") if l]
            assert len(lines) >= 1

    def test_missing_input_returns_1(self, tmp_path: Path) -> None:
        code = main([str(tmp_path / "nope.jsonl"), str(tmp_path / "out")])
        assert code == 1

    def test_custom_prefix(self, tmp_path: Path) -> None:
        records = [{"cat": "a"}, {"cat": "b"}]
        inp = self._write_input(tmp_path, records)
        out = tmp_path / "parts"
        code = main([str(inp), str(out), "--field", "cat", "--prefix", "cat"])
        assert code == 0
        files = list(out.glob("cat_*.jsonl"))
        assert len(files) == 2
