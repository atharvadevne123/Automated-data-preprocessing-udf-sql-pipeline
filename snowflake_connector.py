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


def batch_execute(
    conn: Any,
    sql: str,
    data: Sequence[Sequence[Any]],
    batch_size: int = 1000,
) -> int:
    """Split *data* into batches and call executemany for each batch.

    Useful when a single executemany would exhaust server memory for large
    data sets.  Each batch is committed separately.

    Args:
        conn: Open Snowflake connection.
        sql: Parameterised SQL statement.
        data: Full set of row tuples to insert.
        batch_size: Number of rows per batch (default 1000).

    Returns:
        Total number of rows affected.

    Raises:
        Exception: if any batch fails.
    """
    total = 0
    for i in range(0, len(data), batch_size):
        chunk = data[i : i + batch_size]
        total += execute_many(conn, sql, chunk)
        logger.info("batch_execute: committed batch %d/%d (%d rows)", i // batch_size + 1, -(-len(data) // batch_size), len(chunk))
    logger.info("batch_execute: %d total rows inserted", total)
    return total


def execute_many(conn: Any, sql: str, data: Sequence[Sequence[Any]]) -> int:
    """Execute *sql* with multiple rows of bind parameters (bulk insert).

    Args:
        conn: Open Snowflake connection.
        sql: Parameterised SQL INSERT/UPDATE statement.
        data: Sequence of row tuples or lists to bind.

    Returns:
        Number of rows affected.

    Raises:
        Exception: on execution failure.
    """
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, data)
        affected = cursor.rowcount if cursor.rowcount is not None else len(data)
        logger.info("execute_many: %d row(s) affected via %s", affected, sql[:80])
        return affected
    except Exception as e:
        logger.error("execute_many failed: %s | error: %s", sql[:200], e)
        raise
    finally:
        cursor.close()


def table_exists(conn: Any, table_name: str, schema: str | None = None) -> bool:
    """Return True if *table_name* exists in the current database/schema.

    Args:
        conn: Open Snowflake connection.
        table_name: Name of the table to check (case-insensitive).
        schema: Optional schema name; uses the connection default if None.

    Returns:
        True if the table exists, False otherwise.
    """
    table_upper = table_name.upper()
    try:
        if schema:
            rows = execute_query(
                conn,
                "SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = %s",
                (table_upper, schema.upper()),
            )
        else:
            rows = execute_query(
                conn,
                "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                (table_upper,),
            )
        return len(rows) > 0
    except Exception as e:
        logger.error("table_exists check failed for %s: %s", table_name, e)
        return False


def get_table_row_count(conn: Any, table_name: str) -> int:
    """Return the number of rows in *table_name*.

    Args:
        conn: Open Snowflake connection.
        table_name: Table to count rows in.

    Returns:
        Row count as an integer.
    """
    rows = execute_query(conn, f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
    return int(rows[0][0]) if rows else 0
