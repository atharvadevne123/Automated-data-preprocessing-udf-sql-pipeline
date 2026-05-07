"""Snowflake connection utility — reads credentials from environment / .env file."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Generator, Iterator, Sequence

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
    params: dict[str, Any] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        params["role"] = role
    return params


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


@contextmanager
def managed_connection() -> Generator[Any, None, None]:
    """Context manager that opens and closes a Snowflake connection automatically.

    Usage::

        with managed_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
    """
    conn = get_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
            logger.info("Snowflake connection closed.")
        except Exception as e:
            logger.warning("Error closing Snowflake connection: %s", e)


def execute_query(conn: Any, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
    """Execute *sql* with optional *params* and return all rows.

    Args:
        conn: Open Snowflake connection.
        sql: SQL statement to execute.
        params: Optional sequence of bind parameters.

    Returns:
        List of result tuples.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or ())
        return cursor.fetchall()
    except Exception as e:
        logger.error("Query failed: %s | error: %s", sql[:200], e)
        raise
    finally:
        cursor.close()


def health_check(conn: Any) -> bool:
    """Return True if the Snowflake connection is healthy.

    Runs a lightweight ``SELECT 1`` query to confirm the connection is alive.

    Args:
        conn: Open Snowflake connection.

    Returns:
        True on success, False on any error.
    """
    try:
        rows = execute_query(conn, "SELECT 1")
        return len(rows) == 1
    except Exception as e:
        logger.error("Health check failed: %s", e)
        return False


def get_connection_iterator(conn: Any, sql: str, params: Sequence[Any] | None = None) -> Iterator[tuple]:
    """Yield rows one at a time to avoid loading large result sets into memory.

    Args:
        conn: Open Snowflake connection.
        sql: SQL statement to execute.
        params: Optional sequence of bind parameters.

    Yields:
        Individual result tuples.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or ())
        for row in cursor:
            yield row
    except Exception as e:
        logger.error("Streaming query failed: %s | error: %s", sql[:200], e)
        raise
    finally:
        cursor.close()
