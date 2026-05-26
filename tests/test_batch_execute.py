"""Tests for snowflake_connector.batch_execute()."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from snowflake_connector import batch_execute


def _make_mock_conn(rowcount: int = 1) -> MagicMock:
    conn = MagicMock()
    cursor = MagicMock()
    cursor.rowcount = rowcount
    conn.cursor.return_value = cursor
    return conn


class TestBatchExecute:
    def test_single_batch(self):
        conn = _make_mock_conn(rowcount=3)
        data = [(i,) for i in range(3)]
        total = batch_execute(conn, "INSERT INTO t VALUES (%s)", data, batch_size=10)
        assert total == 3

    def test_multiple_batches(self):
        conn = _make_mock_conn(rowcount=2)
        data = [(i,) for i in range(4)]
        total = batch_execute(conn, "INSERT INTO t VALUES (%s)", data, batch_size=2)
        assert total == 4

    def test_empty_data(self):
        conn = _make_mock_conn()
        total = batch_execute(conn, "INSERT INTO t VALUES (%s)", [], batch_size=10)
        assert total == 0

    def test_cursor_executemany_called(self):
        conn = _make_mock_conn(rowcount=1)
        data = [(1,), (2,)]
        batch_execute(conn, "INSERT INTO t VALUES (%s)", data, batch_size=5)
        cursor = conn.cursor()
        cursor.executemany.assert_called()

    def test_batch_size_one(self):
        conn = _make_mock_conn(rowcount=1)
        data = [(i,) for i in range(5)]
        total = batch_execute(conn, "INSERT INTO t VALUES (%s)", data, batch_size=1)
        assert total == 5

    def test_exception_propagates(self):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.executemany.side_effect = RuntimeError("DB error")
        cursor.rowcount = None
        conn.cursor.return_value = cursor
        with pytest.raises(RuntimeError, match="DB error"):
            batch_execute(conn, "INSERT INTO t VALUES (%s)", [(1,)], batch_size=10)

    @pytest.mark.parametrize("batch_size,data_len", [
        (1, 5), (3, 9), (10, 10), (100, 5),
    ])
    def test_parametrized_batch_sizes(self, batch_size, data_len):
        # rowcount reflects actual rows per batch, not batch_size
        rows_per_batch = min(batch_size, data_len)
        conn = _make_mock_conn(rowcount=rows_per_batch)
        data = [(i,) for i in range(data_len)]
        total = batch_execute(conn, "INSERT INTO t VALUES (%s)", data, batch_size=batch_size)
        assert total == data_len
