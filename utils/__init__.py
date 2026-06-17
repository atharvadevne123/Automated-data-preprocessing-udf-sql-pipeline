"""Utility helpers for the pipeline."""

from __future__ import annotations

from utils.cache import CacheStats, DictCache, LRUCache, cached
from utils.file_utils import ensure_dir, file_line_count, file_size_mb, iter_jsonl, safe_read_json, write_jsonl
from utils.hash_utils import content_hash_file, md5_hex, record_fingerprint, sha256_hex, short_id
from utils.logger import configure_root_logger, get_json_logger, get_logger
from utils.metrics import MemoryMetrics, PipelineRunMetrics, SplitMetrics, Timer, TimeSeries, ValidationMetrics
from utils.profiler import FunctionProfiler, profile_memory, timed
from utils.retry import MaxRetriesExceeded, RetryConfig, retry, retry_on_error
from utils.schema_validator import FieldSpec, RecordSchemaValidator
from utils.text_stats import TextStats, compute_text_stats, lexical_diversity, reading_level_estimate, top_n_words
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
    validate_review_id,
    validate_star_rating,
    validate_stars,
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
    # hash utils
    "sha256_hex",
    "md5_hex",
    "record_fingerprint",
    "short_id",
    "content_hash_file",
    # file utils
    "ensure_dir",
    "file_line_count",
    "file_size_mb",
    "iter_jsonl",
    "write_jsonl",
    "safe_read_json",
    # text stats
    "TextStats",
    "compute_text_stats",
    "lexical_diversity",
    "reading_level_estimate",
    "top_n_words",
    # schema validator
    "FieldSpec",
    "RecordSchemaValidator",
    # metrics extras
    "TimeSeries",
    # validators extras
    "validate_stars",
    "validate_review_id",
]
