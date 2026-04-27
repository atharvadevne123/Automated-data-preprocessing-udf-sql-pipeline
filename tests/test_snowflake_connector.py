"""Tests for snowflake_connector.py"""

from unittest.mock import MagicMock, patch

import pytest

from snowflake_connector import get_connection, get_connection_params

_FULL_ENV = {
    "SNOWFLAKE_ACCOUNT": "acct",
    "SNOWFLAKE_USER": "user",
    "SNOWFLAKE_PASSWORD": "pass",
    "SNOWFLAKE_WAREHOUSE": "wh",
    "SNOWFLAKE_DATABASE": "db",
    "SNOWFLAKE_SCHEMA": "PUBLIC",
}


def test_get_connection_params_missing_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raises EnvironmentError when required vars are absent."""
    for var in _FULL_ENV:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(EnvironmentError, match="Missing required environment variables"):
        get_connection_params()


def test_get_connection_params_partial_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raises EnvironmentError listing only the missing variable."""
    for var, val in _FULL_ENV.items():
        monkeypatch.setenv(var, val)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD")
    with pytest.raises(EnvironmentError, match="SNOWFLAKE_PASSWORD"):
        get_connection_params()


def test_get_connection_params_all_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Returns correct dict when all vars are present."""
    for var, val in _FULL_ENV.items():
        monkeypatch.setenv(var, val)
    params = get_connection_params()
    assert params["account"] == "acct"
    assert params["user"] == "user"
    assert params["database"] == "db"
    assert params["schema"] == "PUBLIC"


def test_get_connection_no_snowflake_package(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raises ImportError when snowflake.connector is not installed."""
    for var, val in _FULL_ENV.items():
        monkeypatch.setenv(var, val)
    with patch.dict("sys.modules", {"snowflake": None, "snowflake.connector": None}):
        with pytest.raises(ImportError, match="snowflake-connector-python"):
            get_connection()


def test_get_connection_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Returns connection object when snowflake.connector is available."""
    for var, val in _FULL_ENV.items():
        monkeypatch.setenv(var, val)
    mock_conn = MagicMock()
    mock_connector = MagicMock()
    mock_connector.connect.return_value = mock_conn
    mock_snowflake = MagicMock()
    mock_snowflake.connector = mock_connector
    with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_connector}):
        conn = get_connection()
    assert conn is mock_conn
    mock_connector.connect.assert_called_once()
