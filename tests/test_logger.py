"""Tests for utils/logger.py."""

from __future__ import annotations

import logging

import pytest

from utils.logger import configure_root_logger, get_logger


def test_get_logger_returns_logger() -> None:
    logger = get_logger("test.module")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test.module"


def test_get_logger_default_level_is_info(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    logger = get_logger("test.info")
    assert logger.level == logging.INFO


@pytest.mark.parametrize(
    "level_str,expected",
    [
        ("DEBUG", logging.DEBUG),
        ("INFO", logging.INFO),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
    ],
)
def test_get_logger_respects_level_arg(level_str: str, expected: int) -> None:
    logger = get_logger(f"test.level.{level_str.lower()}", level=level_str)
    assert logger.level == expected


def test_get_logger_respects_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    logger = get_logger("test.env.debug")
    assert logger.level == logging.DEBUG


def test_get_logger_has_handler() -> None:
    logger = get_logger("test.handler")
    assert len(logger.handlers) >= 1


def test_get_logger_handler_is_stream() -> None:
    logger = get_logger("test.stream")
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


@pytest.mark.parametrize(
    "level_str,expected",
    [
        ("WARNING", logging.WARNING),
        ("INFO", logging.INFO),
        ("DEBUG", logging.DEBUG),
        ("ERROR", logging.ERROR),
    ],
)
def test_configure_root_logger_sets_level(level_str: str, expected: int) -> None:
    configure_root_logger(level_str)
    root = logging.getLogger()
    assert root.level == expected


def test_get_logger_invalid_level_falls_back_to_info() -> None:
    logger = get_logger("test.invalid", level="NOTAREAL")
    assert logger.level == logging.INFO
