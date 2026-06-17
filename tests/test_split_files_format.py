"""Tests for split_files.py --format / ext parameter."""

from __future__ import annotations

from pathlib import Path

from split_files import split_file


def _make_jsonl(path: Path, n: int = 10) -> Path:
    path.write_text("\n".join(f'{{"id": {i}}}' for i in range(n)) + "\n", encoding="utf-8")
    return path


class TestSplitFileFormat:
    def test_default_ext_is_json(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 10)
        files = split_file(src, str(tmp_path / "chunk_"), 2)
        assert all(f.endswith(".json") for f in files)

    def test_jsonl_ext(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 10)
        files = split_file(src, str(tmp_path / "chunk_"), 2, ext="jsonl")
        assert all(f.endswith(".jsonl") for f in files)

    def test_txt_ext(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 6)
        files = split_file(src, str(tmp_path / "chunk_"), 3, ext="txt")
        assert all(f.endswith(".txt") for f in files)

    def test_output_count_matches(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 12)
        files = split_file(src, str(tmp_path / "chunk_"), 4, ext="jsonl")
        assert len(files) == 4

    def test_files_exist_on_disk(self, tmp_path: Path) -> None:
        src = _make_jsonl(tmp_path / "input.jsonl", 5)
        files = split_file(src, str(tmp_path / "out_"), 2, ext="jsonl")
        for f in files:
            assert Path(f).exists()

    def test_all_lines_preserved(self, tmp_path: Path) -> None:
        n = 15
        src = _make_jsonl(tmp_path / "input.jsonl", n)
        files = split_file(src, str(tmp_path / "chunk_"), 3, ext="jsonl")
        total = sum(len(Path(f).read_text().strip().split("\n")) for f in files)
        assert total == n
