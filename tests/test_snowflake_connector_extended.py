"""Extended tests for snowflake_connector — full coverage pass."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from snowflake_connector import get_connection, get_connection_params

_BASE_ENV = {
    "SNOWFLAKE_ACCOUNT": "test-acct",
    "SNOWFLAKE_USER": "test-user",
    "SNOWFLAKE_PASSWORD": "test-pass",
    "SNOWFLAKE_WAREHOUSE": "test-wh",
    "SNOWFLAKE_DATABASE": "test-db",
    "SNOWFLAKE_SCHEMA": "DEV",
}


def _patch_connector(mock_conn: MagicMock) -> dict:
    mock_connector = MagicMock()
    mock_connector.connect.return_value = mock_conn
    mock_snowflake = MagicMock()
    mock_snowflake.connector = mock_connector
    return {"snowflake": mock_snowflake, "snowflake.connector": mock_connector}


def test_get_connection_passes_all_params_to_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, val in _BASE_ENV.items():
        monkeypatch.setenv(var, val)
    monkeypatch.delenv("SNOWFLAKE_ROLE", raising=False)
    mock_conn = MagicMock()
    mock_connector = MagicMock()
    mock_connector.connect.return_value = mock_conn
    mock_snowflake = MagicMock()
    mock_snowflake.connector = mock_connector
    with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_connector}):
        get_connection()
    call_kwargs = mock_connector.connect.call_args[1]
    assert call_kwargs["account"] == "test-acct"
    assert call_kwargs["database"] == "test-db"
    assert call_kwargs["schema"] == "DEV"


def test_get_connection_includes_role_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, val in _BASE_ENV.items():
        monkeypatch.setenv(var, val)
    monkeypatch.setenv("SNOWFLAKE_ROLE", "ANALYST")
    mock_conn = MagicMock()
    modules = _patch_connector(mock_conn)
    with patch.dict("sys.modules", modules):
        conn = get_connection()
    assert conn is mock_conn


def test_get_connection_logs_account_and_database(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    for var, val in _BASE_ENV.items():
        monkeypatch.setenv(var, val)
    mock_conn = MagicMock()
    modules = _patch_connector(mock_conn)
    with patch.dict("sys.modules", modules):
        with caplog.at_level(logging.INFO, logger="snowflake_connector"):
            get_connection()
    assert any("test-acct" in r.message or "test-db" in r.message for r in caplog.records)


def test_get_connection_logs_on_failure(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    import logging

    for var, val in _BASE_ENV.items():
        monkeypatch.setenv(var, val)
    mock_connector = MagicMock()
    mock_connector.connect.side_effect = RuntimeError("auth failed")
    mock_snowflake = MagicMock()
    mock_snowflake.connector = mock_connector
    with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_connector}):
        with caplog.at_level(logging.ERROR, logger="snowflake_connector"):
            with pytest.raises(RuntimeError):
                get_connection()
    assert any("auth failed" in r.message for r in caplog.records)


def test_get_connection_params_password_value(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, val in _BASE_ENV.items():
        monkeypatch.setenv(var, val)
    params = get_connection_params()
    assert params["password"] == "test-pass"


def test_get_connection_params_warehouse_value(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, val in _BASE_ENV.items():
        monkeypatch.setenv(var, val)
    params = get_connection_params()
    assert params["warehouse"] == "test-wh"


@pytest.mark.parametrize("schema", ["PUBLIC", "DEV", "ANALYTICS", "RAW"])
def test_get_connection_params_various_schemas(monkeypatch: pytest.MonkeyPatch, schema: str) -> None:
    for var, val in _BASE_ENV.items():
        monkeypatch.setenv(var, val)
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", schema)
    params = get_connection_params()
    assert params["schema"] == schema
