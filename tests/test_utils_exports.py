"""Tests that verify all public exports in utils/__init__.py are importable."""

from __future__ import annotations

import pytest


class TestUtilsExports:
    def test_get_logger_importable(self):
        from utils import get_logger

        assert callable(get_logger)

    def test_get_json_logger_importable(self):
        from utils import get_json_logger

        assert callable(get_json_logger)

    def test_configure_root_logger_importable(self):
        from utils import configure_root_logger

        assert callable(configure_root_logger)

    def test_validation_error_importable(self):
        from utils import ValidationError

        assert issubclass(ValidationError, ValueError)

    def test_validate_input_path_importable(self):
        from utils import validate_input_path

        assert callable(validate_input_path)

    def test_validate_num_files_importable(self):
        from utils import validate_num_files

        assert callable(validate_num_files)

    def test_validate_star_rating_importable(self):
        from utils import validate_star_rating

        assert callable(validate_star_rating)

    def test_validate_email_importable(self):
        from utils import validate_email

        assert callable(validate_email)

    def test_sanitize_text_importable(self):
        from utils import sanitize_text

        assert callable(sanitize_text)

    def test_split_metrics_importable(self):
        from utils import SplitMetrics

        m = SplitMetrics(input_file="test.jsonl")
        assert m.total_lines == 0

    def test_timer_importable(self):
        from utils import Timer

        with Timer() as t:
            pass
        assert t.elapsed >= 0

    def test_validation_metrics_importable(self):
        from utils import ValidationMetrics

        v = ValidationMetrics(total=10, valid=8, invalid=2)
        assert v.error_rate == pytest.approx(0.2)

    def test_pipeline_run_metrics_importable(self):
        from utils import PipelineRunMetrics

        p = PipelineRunMetrics(pipeline_name="test")
        assert p.throughput == 0.0

    def test_retry_importable(self):
        from utils import retry

        assert callable(retry)

    def test_retry_on_error_importable(self):
        from utils import retry_on_error

        assert callable(retry_on_error)

    def test_max_retries_exceeded_importable(self):
        from utils import MaxRetriesExceeded

        assert issubclass(MaxRetriesExceeded, Exception)

    def test_lru_cache_importable(self):
        from utils import LRUCache

        c = LRUCache(lambda x: x * 2, maxsize=10)
        assert c(5) == 10

    def test_cache_stats_importable(self):
        from utils import CacheStats

        s = CacheStats()
        assert s.hit_rate == 0.0

    def test_cached_importable(self):
        from utils import cached

        assert callable(cached)

    def test_all_exports_list(self):
        import utils

        assert hasattr(utils, "__all__")
        assert len(utils.__all__) >= 20

    def test_coerce_record_importable(self):
        from utils import coerce_record

        result = coerce_record({"a": 1}, ["a"])
        assert result["a"] == 1

    def test_validate_yelp_review_record_importable(self):
        from utils import validate_yelp_review_record

        r = validate_yelp_review_record({"review_id": "r1", "user_id": "u1", "business_id": "b1", "stars": 4.0})
        assert r["review_id"] == "r1"
