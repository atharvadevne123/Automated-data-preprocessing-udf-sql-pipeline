"""Tests for RetryConfig dataclass in utils/retry.py."""

from __future__ import annotations

import pytest

from utils.retry import MaxRetriesExceeded, RetryConfig


class TestRetryConfig:
    def test_default_values(self):
        cfg = RetryConfig()
        assert cfg.max_attempts == 3
        assert cfg.delay == 1.0
        assert cfg.backoff == 2.0
        assert cfg.jitter == 0.0

    def test_custom_values(self):
        cfg = RetryConfig(max_attempts=5, delay=0.1, backoff=1.5)
        assert cfg.max_attempts == 5
        assert cfg.delay == pytest.approx(0.1)

    def test_as_decorator_returns_callable(self):
        cfg = RetryConfig(max_attempts=2, delay=0)
        dec = cfg.as_decorator()
        assert callable(dec)

    def test_as_decorator_retries(self):
        call_count = {"n": 0}

        cfg = RetryConfig(exceptions=(ValueError,), max_attempts=3, delay=0)

        @cfg.as_decorator()
        def flaky():
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise ValueError("temporary")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count["n"] == 2

    def test_as_decorator_exhausts_attempts(self):
        cfg = RetryConfig(exceptions=(RuntimeError,), max_attempts=2, delay=0)

        @cfg.as_decorator()
        def always_fails():
            raise RuntimeError("boom")

        with pytest.raises(MaxRetriesExceeded):
            always_fails()

    def test_non_matching_exception_propagates(self):
        cfg = RetryConfig(exceptions=(ValueError,), max_attempts=3, delay=0)

        @cfg.as_decorator()
        def raises_type_error():
            raise TypeError("wrong type")

        with pytest.raises(TypeError):
            raises_type_error()

    @pytest.mark.parametrize("max_attempts", [1, 2, 5])
    def test_parametrized_max_attempts(self, max_attempts):
        cfg = RetryConfig(max_attempts=max_attempts)
        assert cfg.max_attempts == max_attempts
