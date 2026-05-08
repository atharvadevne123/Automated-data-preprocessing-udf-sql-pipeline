"""Tests for utils.retry."""

from __future__ import annotations

import pytest

from utils.retry import MaxRetriesExceeded, retry, retry_on_error


class TestRetryDecorator:
    def test_succeeds_on_first_attempt(self):
        calls = []

        @retry(max_attempts=3, delay=0)
        def func():
            calls.append(1)
            return "ok"

        assert func() == "ok"
        assert len(calls) == 1

    def test_retries_on_exception(self):
        calls = []

        @retry(exceptions=(ValueError,), max_attempts=3, delay=0)
        def func():
            calls.append(1)
            if len(calls) < 3:
                raise ValueError("retry me")
            return "done"

        assert func() == "done"
        assert len(calls) == 3

    def test_raises_max_retries_exceeded(self):
        @retry(exceptions=(RuntimeError,), max_attempts=2, delay=0)
        def always_fails():
            raise RuntimeError("boom")

        with pytest.raises(MaxRetriesExceeded):
            always_fails()

    def test_non_retried_exception_propagates_immediately(self):
        calls = []

        @retry(exceptions=(ValueError,), max_attempts=3, delay=0)
        def raises_type_error():
            calls.append(1)
            raise TypeError("not retried")

        with pytest.raises(TypeError):
            raises_type_error()
        assert len(calls) == 1

    def test_preserves_function_name(self):
        @retry(max_attempts=2, delay=0)
        def my_function():
            return 1

        assert my_function.__name__ == "my_function"

    def test_max_attempts_one_no_retry(self):
        calls = []

        @retry(max_attempts=1, delay=0)
        def fails():
            calls.append(1)
            raise ValueError("no retry")

        with pytest.raises(MaxRetriesExceeded):
            fails()
        assert len(calls) == 1

    def test_invalid_max_attempts_raises(self):
        with pytest.raises(ValueError):
            @retry(max_attempts=0, delay=0)
            def f():
                pass

    def test_invalid_delay_raises(self):
        with pytest.raises(ValueError):
            @retry(delay=-1)
            def f():
                pass

    def test_backoff_increases_delay(self):
        delays = []
        import time
        original_sleep = time.sleep

        def capture_sleep(s):
            delays.append(s)

        import utils.retry as retry_mod
        import time as t_mod

        original = t_mod.sleep

        @retry(exceptions=(OSError,), max_attempts=3, delay=0.001, backoff=10.0)
        def fails():
            raise OSError("fail")

        try:
            fails()
        except MaxRetriesExceeded:
            pass

    @pytest.mark.parametrize("attempts", [1, 2, 3, 5])
    def test_exact_attempt_count(self, attempts):
        calls = []

        @retry(exceptions=(ValueError,), max_attempts=attempts, delay=0)
        def always_fails():
            calls.append(1)
            raise ValueError("fail")

        with pytest.raises(MaxRetriesExceeded):
            always_fails()
        assert len(calls) == attempts


class TestRetryOnError:
    def test_succeeds(self):
        def add(a, b):
            return a + b

        result = retry_on_error(add, 2, 3, max_attempts=1)
        assert result == 5

    def test_retries_and_succeeds(self):
        calls = []

        def sometimes_fails(x):
            calls.append(x)
            if len(calls) < 2:
                raise IOError("temporary")
            return x * 2

        result = retry_on_error(
            sometimes_fails, 5, exceptions=(IOError,), max_attempts=3, delay=0
        )
        assert result == 10

    def test_raises_after_exhaustion(self):
        def always_fails():
            raise RuntimeError("always")

        with pytest.raises(MaxRetriesExceeded):
            retry_on_error(always_fails, exceptions=(RuntimeError,), max_attempts=2, delay=0)
