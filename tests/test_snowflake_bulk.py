"""Tests for execute_many, table_exists, get_table_row_count in snowflake_connector."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from snowflake_connector import execute_many, get_table_row_count, table_exists


class TestExecuteMany:
    def _make_conn(self, rowcount=3):
        cursor = MagicMock()
        cursor.rowcount = rowcount
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_calls_executemany(self):
        conn, cursor = self._make_conn()
        data = [(1, "a"), (2, "b"), (3, "c")]
        execute_many(conn, "INSERT INTO t VALUES (%s, %s)", data)
        cursor.executemany.assert_called_once_with("INSERT INTO t VALUES (%s, %s)", data)

    def test_returns_rowcount(self):
        conn, cursor = self._make_conn(rowcount=5)
        result = execute_many(conn, "INSERT INTO t VALUES (%s)", [(i,) for i in range(5)])
        assert result == 5

    def test_cursor_always_closed(self):
        conn, cursor = self._make_conn()
        execute_many(conn, "INSERT INTO t VALUES (%s)", [(1,)])
        cursor.close.assert_called_once()

    def test_raises_on_db_error(self):
        conn, cursor = self._make_conn()
        cursor.executemany.side_effect = RuntimeError("db error")
        with pytest.raises(RuntimeError, match="db error"):
            execute_many(conn, "INSERT INTO t VALUES (%s)", [(1,)])

    def test_cursor_closed_even_on_error(self):
        conn, cursor = self._make_conn()
        cursor.executemany.side_effect = RuntimeError("db error")
        try:
            execute_many(conn, "INSERT INTO t VALUES (%s)", [(1,)])
        except RuntimeError:
            pass
        cursor.close.assert_called_once()

    def test_empty_data(self):
        conn, cursor = self._make_conn(rowcount=0)
        result = execute_many(conn, "INSERT INTO t VALUES (%s)", [])
        assert result == 0

    @pytest.mark.parametrize("n_rows", [1, 10, 100])
    def test_various_row_counts(self, n_rows):
        conn, cursor = self._make_conn(rowcount=n_rows)
        data = [(i,) for i in range(n_rows)]
        result = execute_many(conn, "INSERT INTO t VALUES (%s)", data)
        assert result == n_rows


class TestTableExists:
    def _conn_returning(self, rows):
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn

    def test_returns_true_when_table_found(self):
        conn = self._conn_returning([(1,)])
        assert table_exists(conn, "MY_TABLE") is True

    def test_returns_false_when_not_found(self):
        conn = self._conn_returning([])
        assert table_exists(conn, "MISSING") is False

    def test_with_schema_param(self):
        conn = self._conn_returning([(1,)])
        result = table_exists(conn, "MY_TABLE", schema="PUBLIC")
        assert result is True

    def test_returns_false_on_exception(self):
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("connection lost")
        conn = MagicMock()
        conn.cursor.return_value = cursor
        result = table_exists(conn, "ANY_TABLE")
        assert result is False


class TestGetTableRowCount:
    def test_returns_count(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [(42,)]
        conn = MagicMock()
        conn.cursor.return_value = cursor
        assert get_table_row_count(conn, "my_table") == 42

    def test_returns_zero_on_empty_result(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cursor
        assert get_table_row_count(conn, "my_table") == 0
