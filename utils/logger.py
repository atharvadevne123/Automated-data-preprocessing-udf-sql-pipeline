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

    Args:
        level: Log level string.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )
