"""Structured logging configuration for the pipeline."""

from __future__ import annotations

import logging
import os
import sys


def get_logger(name: str, level: str | None = None) -> logging.Logger:
    """Return a consistently configured logger.

    Args:
        name: Logger name, typically ``__name__``.
        level: Optional log level string (DEBUG/INFO/WARNING/ERROR).
               Defaults to the ``LOG_LEVEL`` env var, then INFO.

    Returns:
        Configured :class:`logging.Logger` instance.
    """
    log_level_str = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, log_level_str, logging.INFO)

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(numeric_level)
    return logger


def configure_root_logger(level: str = "INFO") -> None:
    """Configure the root logger with a standard format.

    Call once at application startup (e.g. in ``main()``).
    Safe to call multiple times — always updates the root logger level.

    Args:
        level: Log level string.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric_level)
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        root.addHandler(handler)


def get_json_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a logger that emits newline-delimited JSON log records.

    Each log record is written as a single JSON object with keys:
    ``timestamp``, ``level``, ``logger``, and ``message``.

    Args:
        name: Logger name, typically ``__name__``.
        level: Log level string (default ``"INFO"``).

    Returns:
        Configured :class:`logging.Logger`.
    """
    import json as _json

    class _JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                payload["exception"] = self.formatException(record.exc_info)
            return _json.dumps(payload)

    log = logging.getLogger(name)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_JsonFormatter())
        log.addHandler(handler)
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    return log
