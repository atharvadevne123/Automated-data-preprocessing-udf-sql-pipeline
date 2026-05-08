"""Utility helpers for the pipeline."""

from __future__ import annotations

from utils.cache import CacheStats, LRUCache, cached
from utils.logger import configure_root_logger, get_json_logger, get_logger
from utils.metrics import PipelineRunMetrics, SplitMetrics, Timer, ValidationMetrics
from utils.retry import MaxRetriesExceeded, retry, retry_on_error
from utils.validators import (
    ValidationError,
    coerce_record,
    sanitize_text,
    validate_email,
    validate_input_path,
    validate_jsonl_file,
    validate_num_files,
    validate_output_prefix,
    validate_star_rating,
    validate_yelp_review_record,
)

__all__ = [
    # logger
    "get_logger",
    "get_json_logger",
    "configure_root_logger",
    # validators
    "ValidationError",
    "validate_input_path",
    "validate_num_files",
    "validate_jsonl_file",
    "validate_output_prefix",
    "validate_star_rating",
    "validate_email",
    "sanitize_text",
    "validate_yelp_review_record",
    "coerce_record",
    # metrics
    "SplitMetrics",
    "Timer",
    "ValidationMetrics",
    "PipelineRunMetrics",
    # retry
    "retry",
    "retry_on_error",
    "MaxRetriesExceeded",
    # cache
    "LRUCache",
    "CacheStats",
    "cached",
]
