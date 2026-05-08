"""Extended tests for utils.retry — retry_on_error and edge cases."""

from __future__ import annotations

import pytest

from utils.retry import MaxRetriesExceeded, retry, retry_on_error


class TestRetryWithKwargs:
    def test_kwargs_forwarded(self):
        @retry(max_attempts=1, delay=0)
        def func(a, b=10):
            return a + b

        assert func(5, b=3) == 8

    def test_return_value_preserved(self):
        @retry(max_attempts=2, delay=0)
        def func():
            return {"key": "value"}

        result = func()
        assert result == {"key": "value"}

    def test_retry_with_oserror(self):
        calls = []

        @retry(exceptions=(OSError,), max_attempts=3, delay=0)
        def flakey():
            calls.append(1)
            if len(calls) < 3:
                raise OSError("temporary")
            return "ok"

        assert flakey() == "ok"
        assert len(calls) == 3

    def test_last_error_in_message(self):
        @retry(exceptions=(ValueError,), max_attempts=2, delay=0)
        def raises():
            raise ValueError("specific error")

        with pytest.raises(MaxRetriesExceeded) as exc_info:
            raises()
        assert "specific error" in str(exc_info.value)


class TestRetryOnErrorCoverage:
    def test_with_keyword_args(self):
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        result = retry_on_error(greet, "World", greeting="Hi", max_attempts=1)
        assert result == "Hi, World!"

    @pytest.mark.parametrize("retries,fail_on", [
        (2, 1), (3, 2), (4, 3),
    ])
    def test_succeeds_on_nth_attempt(self, retries, fail_on):
        calls = []

        def sometimes_fails():
            calls.append(1)
            if len(calls) <= fail_on:
                raise IOError("retry")
            return "done"

        result = retry_on_error(
            sometimes_fails, exceptions=(IOError,), max_attempts=retries + 1, delay=0
        )
        assert result == "done"
        assert len(calls) == fail_on + 1


class TestMaxRetriesExceeded:
    def test_is_exception_subclass(self):
        assert issubclass(MaxRetriesExceeded, Exception)

    def test_stores_message(self):
        exc = MaxRetriesExceeded("test message")
        assert "test message" in str(exc)

    def test_can_be_caught_as_exception(self):
        @retry(exceptions=(ValueError,), max_attempts=1, delay=0)
        def fails():
            raise ValueError("x")

        with pytest.raises(Exception):
            fails()


class TestRetryDecoratorProperties:
    def test_wraps_preserves_docstring(self):
        @retry(max_attempts=1, delay=0)
        def documented():
            """My docstring."""
            pass

        assert documented.__doc__ == "My docstring."

    def test_wraps_preserves_module(self):
        @retry(max_attempts=1, delay=0)
        def func():
            pass

        assert func.__module__ == __name__

    @pytest.mark.parametrize("exc_type", [ValueError, RuntimeError, IOError, OSError])
    def test_various_exception_types(self, exc_type):
        calls = []

        @retry(exceptions=(exc_type,), max_attempts=2, delay=0)
        def raises_once():
            calls.append(1)
            if len(calls) == 1:
                raise exc_type("retry")
            return "ok"

        assert raises_once() == "ok"
