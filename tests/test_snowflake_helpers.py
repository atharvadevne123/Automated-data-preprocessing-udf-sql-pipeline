"""Tests for execute_query, health_check, managed_connection, and get_connection_iterator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from snowflake_connector import (
    execute_query,
    get_connection_iterator,
    get_connection_params,
    health_check,
    managed_connection,
)

_FULL_ENV = {
    "SNOWFLAKE_ACCOUNT": "acct",
    "SNOWFLAKE_USER": "user",
    "SNOWFLAKE_PASSWORD": "pass",
    "SNOWFLAKE_WAREHOUSE": "wh",
    "SNOWFLAKE_DATABASE": "db",
    "SNOWFLAKE_SCHEMA": "PUBLIC",
}


def _mock_conn(rows: list[tuple]) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.__iter__ = MagicMock(return_value=iter(rows))
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_execute_query_returns_rows() -> None:
    rows = [(1, "a"), (2, "b")]
    conn = _mock_conn(rows)
    result = execute_query(conn, "SELECT * FROM t")
    assert result == rows


def test_execute_query_passes_params() -> None:
    conn = _mock_conn([(42,)])
    execute_query(conn, "SELECT * FROM t WHERE id = %s", (42,))
    cursor = conn.cursor()
    cursor.execute.assert_called_with("SELECT * FROM t WHERE id = %s", (42,))


def test_execute_query_closes_cursor_on_success() -> None:
    conn = _mock_conn([(1,)])
    execute_query(conn, "SELECT 1")
    conn.cursor().close.assert_called()


def test_execute_query_closes_cursor_on_error() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("bad sql")
    conn = MagicMock()
    conn.cursor.return_value = cursor
    with pytest.raises(RuntimeError):
        execute_query(conn, "BAD SQL")
    cursor.close.assert_called()


def test_health_check_returns_true_on_success() -> None:
    conn = _mock_conn([(1,)])
    assert health_check(conn) is True


def test_health_check_returns_false_on_error() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("timeout")
    conn = MagicMock()
    conn.cursor.return_value = cursor
    assert health_check(conn) is False


def test_managed_connection_closes_on_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, val in _FULL_ENV.items():
        monkeypatch.setenv(var, val)
    mock_conn = MagicMock()
    mock_connector = MagicMock()
    mock_connector.connect.return_value = mock_conn
    mock_snowflake = MagicMock()
    mock_snowflake.connector = mock_connector
    with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_connector}):
        with managed_connection() as conn:
            assert conn is mock_conn
    mock_conn.close.assert_called_once()


def test_managed_connection_closes_even_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, val in _FULL_ENV.items():
        monkeypatch.setenv(var, val)
    mock_conn = MagicMock()
    mock_connector = MagicMock()
    mock_connector.connect.return_value = mock_conn
    mock_snowflake = MagicMock()
    mock_snowflake.connector = mock_connector
    with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_connector}):
        with pytest.raises(ValueError):
            with managed_connection():
                raise ValueError("inner error")
    mock_conn.close.assert_called_once()


def test_get_connection_iterator_yields_rows() -> None:
    rows = [(1,), (2,), (3,)]
    conn = _mock_conn(rows)
    result = list(get_connection_iterator(conn, "SELECT id FROM t"))
    assert result == rows


def test_get_connection_iterator_closes_cursor() -> None:
    rows = [(1,), (2,)]
    conn = _mock_conn(rows)
    list(get_connection_iterator(conn, "SELECT 1"))
    conn.cursor().close.assert_called()


def test_get_connection_params_includes_role_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, val in _FULL_ENV.items():
        monkeypatch.setenv(var, val)
    monkeypatch.setenv("SNOWFLAKE_ROLE", "SYSADMIN")
    params = get_connection_params()
    assert params.get("role") == "SYSADMIN"


def test_get_connection_params_excludes_role_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, val in _FULL_ENV.items():
        monkeypatch.setenv(var, val)
    monkeypatch.delenv("SNOWFLAKE_ROLE", raising=False)
    params = get_connection_params()
    assert "role" not in params
