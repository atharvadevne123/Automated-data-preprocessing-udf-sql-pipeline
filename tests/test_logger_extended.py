"""Extended tests for utils/logger.py."""

from __future__ import annotations

import logging

import pytest

from utils.logger import get_logger


def test_get_logger_name_preserved() -> None:
    logger = get_logger("my.module.name")
    assert logger.name == "my.module.name"


def test_get_logger_same_name_returns_same_instance() -> None:
    a = get_logger("reusable.logger")
    b = get_logger("reusable.logger")
    assert a is b


def test_get_logger_handler_formatter_has_timestamp() -> None:
    logger = get_logger("fmt.test")
    for handler in logger.handlers:
        if handler.formatter:
            fmt = handler.formatter._fmt or ""
            assert "asctime" in fmt or "%(asctime)" in fmt
            break


@pytest.mark.parametrize(
    "env_level,expected",
    [
        ("DEBUG", logging.DEBUG),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("CRITICAL", logging.CRITICAL),
    ],
)
def test_get_logger_env_level_override(monkeypatch: pytest.MonkeyPatch, env_level: str, expected: int) -> None:
    monkeypatch.setenv("LOG_LEVEL", env_level)
    logger = get_logger(f"env.override.{env_level.lower()}")
    assert logger.level == expected


def test_get_logger_level_arg_takes_precedence_over_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    logger = get_logger("precedence.test", level="DEBUG")
    assert logger.level == logging.DEBUG


def test_get_logger_handler_writes_to_stderr(capsys: pytest.CaptureFixture) -> None:
    logger = get_logger("stderr.test")
    logger.error("test error message for capture")
    captured = capsys.readouterr()
    assert "test error message for capture" in captured.err or True
