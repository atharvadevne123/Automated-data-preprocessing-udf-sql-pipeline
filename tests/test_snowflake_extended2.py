"""Extended tests for snowflake_connector helper functions — mocked layer."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from snowflake_connector import (
    execute_many,
    execute_query,
    get_table_row_count,
    health_check,
    table_exists,
)


def _mock_conn(rows=None, rowcount=None):
    """Return a mock Snowflake connection whose cursor returns *rows*."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    cursor.rowcount = rowcount
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestExecuteQueryMocked:
    def test_returns_rows(self):
        conn, cursor = _mock_conn(rows=[(1,), (2,)])
        cursor.execute.return_value = None
        result = execute_query(conn, "SELECT 1")
        assert result == [(1,), (2,)]

    def test_cursor_closed_on_success(self):
        conn, cursor = _mock_conn(rows=[])
        cursor.execute.return_value = None
        execute_query(conn, "SELECT 1")
        cursor.close.assert_called_once()

    def test_cursor_closed_on_exception(self):
        conn, cursor = _mock_conn()
        cursor.execute.side_effect = RuntimeError("db error")
        with pytest.raises(RuntimeError):
            execute_query(conn, "SELECT 1")
        cursor.close.assert_called_once()

    def test_params_forwarded_to_execute(self):
        conn, cursor = _mock_conn(rows=[])
        cursor.execute.return_value = None
        execute_query(conn, "SELECT %s", ("value",))
        cursor.execute.assert_called_once_with("SELECT %s", ("value",))


class TestExecuteManyMocked:
    def test_returns_rowcount(self):
        conn, cursor = _mock_conn(rowcount=3)
        cursor.executemany.return_value = None
        result = execute_many(conn, "INSERT INTO t VALUES (%s)", [(1,), (2,), (3,)])
        assert result == 3

    def test_falls_back_to_len_data_when_rowcount_none(self):
        conn, cursor = _mock_conn(rowcount=None)
        cursor.executemany.return_value = None
        data = [(1,), (2,), (3,), (4,)]
        result = execute_many(conn, "INSERT INTO t VALUES (%s)", data)
        assert result == 4

    def test_cursor_closed_after_success(self):
        conn, cursor = _mock_conn(rowcount=1)
        cursor.executemany.return_value = None
        execute_many(conn, "INSERT INTO t VALUES (%s)", [(1,)])
        cursor.close.assert_called_once()

    def test_cursor_closed_on_failure(self):
        conn, cursor = _mock_conn()
        cursor.executemany.side_effect = ValueError("bulk error")
        with pytest.raises(ValueError):
            execute_many(conn, "INSERT INTO t VALUES (%s)", [(1,)])
        cursor.close.assert_called_once()

    def test_empty_data_returns_zero(self):
        conn, cursor = _mock_conn(rowcount=0)
        cursor.executemany.return_value = None
        result = execute_many(conn, "INSERT INTO t VALUES (%s)", [])
        assert result == 0


class TestTableExistsMocked:
    def test_returns_true_when_row_found(self):
        conn, cursor = _mock_conn(rows=[(1,)])
        cursor.execute.return_value = None
        assert table_exists(conn, "MY_TABLE") is True

    def test_returns_false_when_no_rows(self):
        conn, cursor = _mock_conn(rows=[])
        cursor.execute.return_value = None
        assert table_exists(conn, "MISSING_TABLE") is False

    def test_returns_false_on_exception(self):
        conn, cursor = _mock_conn()
        cursor.execute.side_effect = RuntimeError("conn error")
        result = table_exists(conn, "ANY_TABLE")
        assert result is False

    def test_schema_passed_in_query(self):
        conn, cursor = _mock_conn(rows=[(1,)])
        cursor.execute.return_value = None
        result = table_exists(conn, "MY_TABLE", schema="MY_SCHEMA")
        assert result is True
        # ensure execute was called with schema param
        call_args = cursor.execute.call_args
        assert "MY_SCHEMA" in str(call_args) or "MY_SCHEMA" in str(cursor.execute.call_args_list)


class TestGetTableRowCountMocked:
    def test_returns_count(self):
        conn, cursor = _mock_conn(rows=[(42,)])
        cursor.execute.return_value = None
        result = get_table_row_count(conn, "MY_TABLE")
        assert result == 42

    def test_empty_result_returns_zero(self):
        conn, cursor = _mock_conn(rows=[])
        cursor.execute.return_value = None
        result = get_table_row_count(conn, "EMPTY_TABLE")
        assert result == 0


class TestHealthCheckMocked:
    def test_returns_true_on_success(self):
        conn, cursor = _mock_conn(rows=[(1,)])
        cursor.execute.return_value = None
        assert health_check(conn) is True

    def test_returns_false_on_exception(self):
        conn, cursor = _mock_conn()
        cursor.execute.side_effect = RuntimeError("conn dead")
        assert health_check(conn) is False
