"""Tests for the utils package public interface."""

from __future__ import annotations

import importlib


def test_utils_package_importable() -> None:
    mod = importlib.import_module("utils")
    assert mod is not None


def test_utils_logger_importable() -> None:
    mod = importlib.import_module("utils.logger")
    assert hasattr(mod, "get_logger")
    assert hasattr(mod, "configure_root_logger")


def test_utils_validators_importable() -> None:
    mod = importlib.import_module("utils.validators")
    assert hasattr(mod, "ValidationError")
    assert hasattr(mod, "validate_input_path")
    assert hasattr(mod, "validate_num_files")
    assert hasattr(mod, "validate_jsonl_file")
    assert hasattr(mod, "validate_output_prefix")
    assert hasattr(mod, "coerce_record")


def test_utils_metrics_importable() -> None:
    mod = importlib.import_module("utils.metrics")
    assert hasattr(mod, "SplitMetrics")
    assert hasattr(mod, "Timer")


def test_scripts_package_importable() -> None:
    mod = importlib.import_module("scripts")
    assert mod is not None


def test_scripts_validate_jsonl_importable() -> None:
    mod = importlib.import_module("scripts.validate_jsonl")
    assert hasattr(mod, "main")


def test_scripts_benchmark_importable() -> None:
    mod = importlib.import_module("scripts.benchmark")
    assert hasattr(mod, "generate_synthetic_jsonl")
    assert hasattr(mod, "benchmark_split")
    assert hasattr(mod, "main")
