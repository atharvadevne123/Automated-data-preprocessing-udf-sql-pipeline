"""Utility helpers for the pipeline."""

from __future__ import annotations

from utils.cache import CacheStats, DictCache, LRUCache, cached
from utils.logger import configure_root_logger, get_json_logger, get_logger
from utils.metrics import MemoryMetrics, PipelineRunMetrics, SplitMetrics, Timer, ValidationMetrics
from utils.profiler import FunctionProfiler, profile_memory, timed
from utils.retry import MaxRetriesExceeded, RetryConfig, retry, retry_on_error
from utils.validators import (
    ValidationError,
    coerce_record,
    sanitize_text,
    validate_business_id_format,
    validate_coordinates,
    validate_date_format,
    validate_email,
    validate_input_path,
    validate_jsonl_file,
    validate_num_files,
    validate_output_prefix,
    validate_star_rating,
    validate_text_length,
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
    "validate_date_format",
    "validate_business_id_format",
    "validate_coordinates",
    "sanitize_text",
    "validate_text_length",
    "validate_yelp_review_record",
    "coerce_record",
    # metrics
    "SplitMetrics",
    "Timer",
    "ValidationMetrics",
    "PipelineRunMetrics",
    "MemoryMetrics",
    # retry
    "retry",
    "retry_on_error",
    "MaxRetriesExceeded",
    "RetryConfig",
    # cache
    "LRUCache",
    "DictCache",
    "CacheStats",
    "cached",
    # profiler
    "FunctionProfiler",
    "timed",
    "profile_memory",
]
