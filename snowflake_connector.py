"""Snowflake connection utility — reads credentials from environment / .env file."""

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv is optional; env vars may be set by other means

_REQUIRED_VARS: list[str] = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]


def get_connection_params() -> dict[str, Any]:
    """Read and validate Snowflake connection parameters from environment variables.

    Raises:
        EnvironmentError: if any required variable is missing.
    """
    missing = [v for v in _REQUIRED_VARS if not os.getenv(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Copy .env.example to .env and fill in your values."
        )
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }


def get_connection() -> Any:
    """Return an open Snowflake connector connection.

    Requires ``snowflake-connector-python``:
        pip install snowflake-connector-python

    Raises:
        ImportError: if snowflake-connector-python is not installed.
        EnvironmentError: if required env vars are missing.
        snowflake.connector.Error: on connection failure.
    """
    try:
        import snowflake.connector  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "snowflake-connector-python is required. "
            "Install it with: pip install snowflake-connector-python"
        ) from exc

    params = get_connection_params()
    logger.info(
        "Connecting to Snowflake: account=%s database=%s",
        params["account"],
        params["database"],
    )
    try:
        conn = snowflake.connector.connect(**params)
        logger.info("Snowflake connection established.")
        return conn
    except Exception as e:
        logger.error("Failed to connect to Snowflake: %s", e)
        raise
