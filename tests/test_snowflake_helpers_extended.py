"""Extended tests for snowflake_connector helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from snowflake_connector import execute_query, get_connection_iterator, health_check


def _mock_conn(rows: list[tuple]) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.__iter__ = MagicMock(return_value=iter(rows))
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "SELECT id, name FROM users WHERE active = 1",
        "SELECT COUNT(*) FROM tbl_yelp_reviews",
    ],
)
def test_execute_query_accepts_various_sql(sql: str) -> None:
    conn = _mock_conn([(1,)])
    result = execute_query(conn, sql)
    assert isinstance(result, list)


def test_execute_query_returns_empty_list_on_no_rows() -> None:
    conn = _mock_conn([])
    result = execute_query(conn, "SELECT 1 WHERE 1=0")
    assert result == []


@pytest.mark.parametrize("row_count", [1, 5, 50, 100])
def test_execute_query_returns_correct_row_count(row_count: int) -> None:
    rows = [(i, f"val_{i}") for i in range(row_count)]
    conn = _mock_conn(rows)
    result = execute_query(conn, "SELECT id, val FROM t")
    assert len(result) == row_count


def test_health_check_uses_select_1() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1,)]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    health_check(conn)
    executed_sql = cursor.execute.call_args[0][0]
    assert "1" in executed_sql


def test_health_check_true_on_single_row() -> None:
    conn = _mock_conn([(1,)])
    assert health_check(conn) is True


def test_health_check_false_on_empty_result() -> None:
    conn = _mock_conn([])
    assert health_check(conn) is False


@pytest.mark.parametrize("n_rows", [0, 1, 5, 20])
def test_get_connection_iterator_row_count(n_rows: int) -> None:
    rows = [(i,) for i in range(n_rows)]
    conn = _mock_conn(rows)
    result = list(get_connection_iterator(conn, "SELECT id FROM t"))
    assert len(result) == n_rows


def test_get_connection_iterator_closes_cursor_on_error() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("fail")
    cursor.__iter__ = MagicMock(side_effect=RuntimeError("fail"))
    conn = MagicMock()
    conn.cursor.return_value = cursor
    with pytest.raises(RuntimeError):
        list(get_connection_iterator(conn, "BAD SQL"))
    cursor.close.assert_called()
