"""Tests for utils/hash_utils.py."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from utils.hash_utils import (
    content_hash_file,
    md5_hex,
    record_fingerprint,
    sha256_hex,
    short_id,
)


class TestSha256Hex:
    def test_known_value(self) -> None:
        result = sha256_hex("hello")
        assert result == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_empty_string(self) -> None:
        result = sha256_hex("")
        assert len(result) == 64

    def test_length_64(self) -> None:
        assert len(sha256_hex("any text")) == 64

    def test_deterministic(self) -> None:
        assert sha256_hex("abc") == sha256_hex("abc")

    def test_different_inputs(self) -> None:
        assert sha256_hex("a") != sha256_hex("b")


class TestMd5Hex:
    def test_length_32(self) -> None:
        assert len(md5_hex("test")) == 32

    def test_deterministic(self) -> None:
        assert md5_hex("abc") == md5_hex("abc")

    def test_different_inputs(self) -> None:
        assert md5_hex("x") != md5_hex("y")


class TestRecordFingerprint:
    def test_same_record_same_fp(self) -> None:
        r = {"a": 1, "b": 2}
        assert record_fingerprint(r) == record_fingerprint(r)

    def test_order_invariant(self) -> None:
        r1 = {"a": 1, "b": 2}
        r2 = {"b": 2, "a": 1}
        assert record_fingerprint(r1) == record_fingerprint(r2)

    def test_key_fields(self) -> None:
        r1 = {"id": "x", "noise": 1}
        r2 = {"id": "x", "noise": 2}
        assert record_fingerprint(r1, ["id"]) == record_fingerprint(r2, ["id"])

    def test_md5_algorithm(self) -> None:
        r = {"k": "v"}
        fp = record_fingerprint(r, algorithm="md5")
        assert len(fp) == 32

    def test_invalid_algorithm(self) -> None:
        with pytest.raises(ValueError, match="algorithm"):
            record_fingerprint({}, algorithm="crc32")

    def test_sha256_algorithm(self) -> None:
        r = {"k": "v"}
        fp = record_fingerprint(r, algorithm="sha256")
        assert len(fp) == 64


class TestShortId:
    def test_default_length(self) -> None:
        assert len(short_id({"x": 1})) == 8

    def test_custom_length(self) -> None:
        assert len(short_id({"x": 1}, length=16)) == 16

    def test_max_length(self) -> None:
        assert len(short_id({"x": 1}, length=64)) == 64

    def test_invalid_length_zero(self) -> None:
        with pytest.raises(ValueError):
            short_id({"x": 1}, length=0)

    def test_invalid_length_too_large(self) -> None:
        with pytest.raises(ValueError):
            short_id({"x": 1}, length=65)


class TestContentHashFile:
    def test_same_file_same_hash(self, tmp_path: Path) -> None:
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        h1 = content_hash_file(str(f))
        h2 = content_hash_file(str(f))
        assert h1 == h2

    def test_different_contents_different_hash(self, tmp_path: Path) -> None:
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("alpha")
        f2.write_text("beta")
        assert content_hash_file(str(f1)) != content_hash_file(str(f2))

    def test_length_64(self, tmp_path: Path) -> None:
        f = tmp_path / "t.txt"
        f.write_text("data")
        assert len(content_hash_file(str(f))) == 64

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(OSError):
            content_hash_file(str(tmp_path / "nonexistent.txt"))
