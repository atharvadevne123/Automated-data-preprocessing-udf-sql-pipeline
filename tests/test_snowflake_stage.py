"""Tests for copy_into_stage and list_stage_files in snowflake_connector.py."""

from __future__ import annotations

from unittest.mock import MagicMock

from snowflake_connector import copy_into_stage, list_stage_files


class TestCopyIntoStage:
    def _make_conn(self, rows=None):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("uploaded", "my_stage", 1024, "UPLOADED")] if rows is None else rows
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_returns_rows(self) -> None:
        conn, cursor = self._make_conn()
        result = copy_into_stage(conn, "@my_stage", "/tmp/data.json")
        assert isinstance(result, list)
        assert len(result) == 1

    def test_sql_contains_put(self) -> None:
        conn, cursor = self._make_conn()
        copy_into_stage(conn, "@my_stage", "/tmp/data.json")
        sql_call = cursor.execute.call_args[0][0]
        assert "PUT" in sql_call
        assert "@my_stage" in sql_call
        assert "/tmp/data.json" in sql_call

    def test_default_file_format_json(self) -> None:
        conn, cursor = self._make_conn()
        copy_into_stage(conn, "@stg", "/tmp/f.json")
        sql_call = cursor.execute.call_args[0][0]
        assert "TYPE=JSON" in sql_call

    def test_custom_file_format(self) -> None:
        conn, cursor = self._make_conn()
        copy_into_stage(conn, "@stg", "/tmp/f.csv", file_format="TYPE=CSV")
        sql_call = cursor.execute.call_args[0][0]
        assert "TYPE=CSV" in sql_call

    def test_cursor_closed(self) -> None:
        conn, cursor = self._make_conn()
        copy_into_stage(conn, "@stg", "/tmp/f.json")
        cursor.close.assert_called_once()

    def test_empty_result(self) -> None:
        conn, cursor = self._make_conn(rows=[])
        result = copy_into_stage(conn, "@stg", "/tmp/f.json")
        assert result == []


class TestListStageFiles:
    def _make_conn(self, rows=None):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("@my_stage/file1.json",), ("@my_stage/file2.json",)] if rows is None else rows
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_returns_list_of_strings(self) -> None:
        conn, cursor = self._make_conn()
        result = list_stage_files(conn, "@my_stage")
        assert isinstance(result, list)
        assert all(isinstance(r, str) for r in result)

    def test_sql_contains_list(self) -> None:
        conn, cursor = self._make_conn()
        list_stage_files(conn, "@my_stage")
        sql_call = cursor.execute.call_args[0][0]
        assert "LIST" in sql_call
        assert "@my_stage" in sql_call

    def test_returns_correct_paths(self) -> None:
        conn, cursor = self._make_conn()
        result = list_stage_files(conn, "@my_stage")
        assert result == ["@my_stage/file1.json", "@my_stage/file2.json"]

    def test_empty_stage(self) -> None:
        conn, cursor = self._make_conn(rows=[])
        result = list_stage_files(conn, "@empty_stage")
        assert result == []

    def test_cursor_closed(self) -> None:
        conn, cursor = self._make_conn()
        list_stage_files(conn, "@my_stage")
        cursor.close.assert_called_once()

    def test_single_file(self) -> None:
        conn, cursor = self._make_conn(rows=[("@stg/only.json",)])
        result = list_stage_files(conn, "@stg")
        assert len(result) == 1
        assert result[0] == "@stg/only.json"
