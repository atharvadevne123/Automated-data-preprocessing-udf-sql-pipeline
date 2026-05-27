"""Tests for get_json_logger() in utils.logger."""

from __future__ import annotations

import io
import json
import logging

import pytest

from utils.logger import get_json_logger


class TestGetJsonLogger:
    def _capture_logger(self, name: str) -> tuple[logging.Logger, io.StringIO]:
        buf = io.StringIO()
        logger = logging.getLogger(name)
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        handler = logging.StreamHandler(buf)

        class _JsonFmt(logging.Formatter):
            def format(self, record):
                import json as _j

                return _j.dumps(
                    {
                        "timestamp": self.formatTime(record),
                        "level": record.levelname,
                        "logger": record.name,
                        "message": record.getMessage(),
                    }
                )

        handler.setFormatter(_JsonFmt())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        return logger, buf

    def test_returns_logger_instance(self):
        logger = get_json_logger("test.json_logger")
        assert isinstance(logger, logging.Logger)

    def test_log_output_is_valid_json(self):
        logger, buf = self._capture_logger("test.json_valid")
        logger.info("hello world")
        output = buf.getvalue().strip()
        data = json.loads(output)
        assert isinstance(data, dict)

    def test_log_output_has_message(self):
        logger, buf = self._capture_logger("test.json_msg")
        logger.info("my message")
        data = json.loads(buf.getvalue().strip())
        assert data["message"] == "my message"

    def test_log_output_has_level(self):
        logger, buf = self._capture_logger("test.json_level")
        logger.warning("warn msg")
        data = json.loads(buf.getvalue().strip())
        assert data["level"] == "WARNING"

    def test_log_output_has_logger_name(self):
        logger, buf = self._capture_logger("test.json_name")
        logger.info("test")
        data = json.loads(buf.getvalue().strip())
        assert data["logger"] == "test.json_name"

    def test_log_output_has_timestamp(self):
        logger, buf = self._capture_logger("test.json_ts")
        logger.info("ts test")
        data = json.loads(buf.getvalue().strip())
        assert "timestamp" in data

    @pytest.mark.parametrize("level_str", ["DEBUG", "INFO", "WARNING", "ERROR"])
    def test_various_log_levels(self, level_str):
        lg = get_json_logger(f"test.json_{level_str.lower()}", level=level_str)
        assert lg.level == getattr(logging, level_str)

    def test_default_level_is_info(self):
        lg = get_json_logger("test.json_default_level_check")
        assert lg.level == logging.INFO

    def test_multiple_calls_same_name_returns_same_logger(self):
        l1 = get_json_logger("test.json_same")
        l2 = get_json_logger("test.json_same")
        assert l1 is l2
