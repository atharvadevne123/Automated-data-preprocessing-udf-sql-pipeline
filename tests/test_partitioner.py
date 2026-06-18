"""Tests for pipeline/partitioner.py."""

from __future__ import annotations

from pathlib import Path

from pipeline.partitioner import FilePartitioner, RecordPartitioner


class TestRecordPartitioner:
    def _make_records(self) -> list[dict]:
        return [
            {"stars": 5, "id": 1},
            {"stars": 4, "id": 2},
            {"stars": 5, "id": 3},
            {"stars": 3, "id": 4},
        ]

    def test_basic_partition(self) -> None:
        p = RecordPartitioner(key_func=lambda r: r["stars"])
        p.add_batch(self._make_records())
        assert len(p.get_partition(5)) == 2
        assert len(p.get_partition(4)) == 1

    def test_partition_keys(self) -> None:
        p = RecordPartitioner(key_func=lambda r: r["stars"])
        p.add_batch(self._make_records())
        keys = p.partition_keys()
        assert set(keys) == {5, 4, 3}

    def test_partition_sizes(self) -> None:
        p = RecordPartitioner(key_func=lambda r: r.get("cat", "x"))
        p.add_batch([{"cat": "a"}, {"cat": "a"}, {"cat": "b"}])
        sizes = p.partition_sizes()
        assert sizes["a"] == 2
        assert sizes["b"] == 1

    def test_missing_key_uses_default(self) -> None:
        p = RecordPartitioner(key_func=lambda r: r.get("missing"))
        p.add({"other": 1})
        assert len(p.get_partition("__other__")) == 1

    def test_custom_default_key(self) -> None:
        p = RecordPartitioner(
            key_func=lambda r: r.get("x"),
            default_key="unknown",
        )
        p.add({"y": 1})
        assert len(p.get_partition("unknown")) == 1

    def test_all_partitions(self) -> None:
        p = RecordPartitioner(key_func=lambda r: r["k"])
        p.add({"k": "a", "v": 1})
        p.add({"k": "b", "v": 2})
        all_p = p.all_partitions()
        assert set(all_p.keys()) == {"a", "b"}

    def test_reset(self) -> None:
        p = RecordPartitioner(key_func=lambda r: r["k"])
        p.add({"k": "a"})
        p.reset()
        assert p.partition_keys() == []

    def test_get_missing_partition_empty(self) -> None:
        p = RecordPartitioner(key_func=lambda r: r.get("k"))
        result = p.get_partition("nonexistent")
        assert result == []

    def test_all_partitions_is_copy(self) -> None:
        p = RecordPartitioner(key_func=lambda r: r["k"])
        p.add({"k": "a"})
        all_p = p.all_partitions()
        all_p["a"].append({"injected": True})
        assert len(p.get_partition("a")) == 1


class TestFilePartitioner:
    def test_flush_creates_files(self, tmp_path: Path) -> None:
        p = FilePartitioner(
            key_func=lambda r: r["stars"],
            output_dir=tmp_path,
            prefix="stars",
        )
        p.add_batch([{"stars": 1, "id": 1}, {"stars": 2, "id": 2}])
        paths = p.flush()
        assert len(paths) == 2
        for path in paths:
            assert path.exists()

    def test_flush_content(self, tmp_path: Path) -> None:
        import json

        p = FilePartitioner(
            key_func=lambda r: r["cat"],
            output_dir=tmp_path,
        )
        p.add({"cat": "a", "val": 42})
        paths = p.flush()
        assert len(paths) == 1
        lines = paths[0].read_text().strip().split("\n")
        assert json.loads(lines[0])["val"] == 42

    def test_creates_output_dir(self, tmp_path: Path) -> None:
        subdir = tmp_path / "sub" / "dir"
        p = FilePartitioner(key_func=lambda r: r.get("k", "x"), output_dir=subdir)
        p.add({"k": "x"})
        p.flush()
        assert subdir.exists()
