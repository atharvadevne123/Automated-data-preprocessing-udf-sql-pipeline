# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.0] — 2026-06-17

### Added
- `pipeline/deduplicator.py` — `RecordDeduplicator` with SHA-256 hashing and `DeduplicationStats`
- `pipeline/sampler.py` — `ReservoirSampler` (Vitter's Algorithm R) and `StratifiedSampler`
- `pipeline/partitioner.py` — `RecordPartitioner` and `FilePartitioner` for JSONL output by key
- `pipeline/normalizer.py` — `FieldNormalizer` with pluggable normalisation functions
- `utils/hash_utils.py` — `sha256_hex`, `md5_hex`, `record_fingerprint`, `short_id`, `content_hash_file`
- `utils/text_stats.py` — `TextStats`, `compute_text_stats`, `lexical_diversity`, `reading_level_estimate`, `top_n_words`
- `utils/schema_validator.py` — `FieldSpec`, `ValidationResult`, `RecordSchemaValidator`
- `utils/file_utils.py` — `iter_jsonl`, `write_jsonl`, `list_jsonl_files`, `file_size_mb`, `ensure_dir`
- `scripts/deduplicate.py` — CLI for deduplicating JSONL files with `--key-fields` and `--stats`
- `scripts/partition_data.py` — CLI for partitioning JSONL files by field value
- `utils/profiler.py` — `AsyncTimer` async context manager for wall-clock timing
- `pipeline/exporter.py` — `DataExporter.to_parquet_compatible()` for Parquet-friendly record flattening
- `snowflake_connector.py` — `copy_into_stage()` and `list_stage_files()` for internal stage management
- `models/yelp.py` — `YelpTip` Pydantic model with `word_count`, `is_valid_date`, `to_json` methods
- `pipeline/aggregator.py` — `percentile_stars()` and `stddev_stars()` on `BusinessStats`
- `pipeline/cleaner.py` — `extract_emails()` and `extract_phones()` static methods on `TextCleaner`
- `utils/validators.py` — `validate_stars()` and `validate_review_id()` validators
- `pipeline/joiner.py` — `inner_join()` and `right_join()` on `RecordJoiner`
- `pipeline/transformer.py` — `ValueMapper` and `FieldDropper` transforms
- `utils/metrics.py` — `TimeSeries` for named time-series metric recording
- `scripts/generate_report.py` — `--sections` flag to select report output sections
- `scripts/run_pipeline.py` — `--workers` flag for parallel cleaning/enrichment via `ProcessPoolExecutor`
- `models/__init__.py` — exports `YelpTip`
- `.github/ISSUE_TEMPLATE/` — bug report and feature request templates
- `.github/PULL_REQUEST_TEMPLATE.md` — standardised PR checklist
- `docs/api_reference.md` — quick-reference for all v1.4.0 additions
- 20+ new test modules covering all new functionality
- `Makefile` — `deduplicate` and `partition` convenience targets

### Fixed
- `.github/workflows/ci.yml` — updated to `actions/checkout@v4`, `actions/setup-python@v5`, `actions/upload-artifact@v4` with pip caching

## [1.3.0] — 2026-05-25

### Added
- `pipeline/transformer.py` — `FieldRenamer`, `TypeCoercer`, `ComputedFieldAdder` for record transformation
- `pipeline/joiner.py` — `RecordJoiner` for left-join operations on JSONL records
- `pipeline/aggregator.py` — `BusinessStats.merge()` and `StatsAggregator.merge()` for parallel aggregation
- `pipeline/sentiment.py` — `analyze_stream()` lazy generator for memory-efficient streaming
- `pipeline/cleaner.py` — `TextCleaner.normalize_whitespace()` static method; `CleanerStats` dataclass; `strip_punctuation` flag
- `pipeline/processor.py` — `process_files_parallel()` for concurrent multi-file processing
- `pipeline/__init__.py` — exports for `CleanerStats`, `RecordJoiner`, `FieldRenamer`, `TypeCoercer`, `ComputedFieldAdder`
- `utils/profiler.py` — `FunctionProfiler`, `timed()` context manager, `profile_memory()` helper
- `utils/metrics.py` — `MemoryMetrics` dataclass with `tracemalloc` integration
- `utils/retry.py` — `RetryConfig` dataclass with configurable backoff and jitter
- `utils/cache.py` — `DictCache` with TTL expiry and maxsize eviction
- `utils/validators.py` — `validate_date_format()`, `validate_business_id_format()`, `validate_coordinates()`, `validate_text_length()`
- `utils/__init__.py` — updated exports for all new utilities
- `snowflake_connector.py` — `batch_execute()` chunked INSERT helper
- `split_files.py` — `--dry-run` preview flag; `_write_chunk()` extracted helper; `_PROGRESS_INTERVAL` module constant
- `scripts/run_pipeline.py` — end-to-end pipeline orchestration CLI
- `scripts/analyze_sentiment.py` — `--output-format` flag (jsonl/json)
- `scripts/generate_report.py` — `--format markdown` output option
- `scripts/validate_jsonl.py` — `--required-fields` schema validation flag
- `scripts/benchmark.py` — `--output-json` flag for persisting metrics
- `models/yelp.py` — `YelpCheckin`, `YelpPhoto` models; `YelpBusiness` latitude/longitude fields; helper methods on all models
- `models/__init__.py` — exports for `YelpCheckin`, `YelpPhoto`
- `.github/workflows/ci.yml` — `bandit` security scan and `ruff format --check` steps
- `pyproject.toml` — `[tool.bandit]` configuration; `run-pipeline` entry point
- 300+ new tests across 20+ test modules

### Changed
- `pipeline/aggregator.py` — extracted `_update_business()` private method
- `pipeline/processor.py` — extracted `_parse_line()` static method
- `pipeline/exporter.py` — added OSError handling with logging in all write methods

### Fixed
- `pipeline/cleaner.py` — removed unused `field` import that shadowed parameter name

## [1.2.0] — 2026-05-08

### Added
- `pipeline/` package: `RecordProcessor`, `TextCleaner`, `SentimentAnalyzer`, `StatsAggregator`, `DataExporter`
- `models/` package: `YelpReview`, `YelpBusiness`, `YelpUser` Pydantic models
- `utils/retry.py` — exponential-backoff retry decorator with `MaxRetriesExceeded`
- `utils/cache.py` — LRU cache wrapper (`LRUCache`, `CacheStats`, `@cached`) with hit/miss stats
- `utils/logger.py` — `get_json_logger()` for structured JSON log output
- `utils/validators.py` — `validate_star_rating()`, `validate_email()`, `sanitize_text()`, `validate_yelp_review_record()`
- `utils/metrics.py` — `ValidationMetrics`, `PipelineRunMetrics` dataclasses
- `utils/__init__.py` — `__all__` exports for the full utils public API
- `split_files.py` — `split_file_stream()` in-memory chunking and `estimate_chunks_for_size()` helper
- `snowflake_connector.py` — `execute_many()`, `table_exists()`, `get_table_row_count()` helpers
- `scripts/analyze_sentiment.py` — CLI for batch JSONL sentiment enrichment
- `scripts/generate_report.py` — CLI for JSON summary report generation
- `ARCHITECTURE.md` — system design document with component diagram
- `docs/pipeline.md` — pipeline module API reference
- `docs/models.md` — Pydantic model API reference
- 200+ new tests across 12 test modules
- Makefile targets: `install-dev`, `analyze-sentiment`, `generate-report`, `pipeline-demo`
- `pyproject.toml`: `[project.scripts]`, `[tool.ruff.format]`, extended classifiers

### Changed
- `pyproject.toml` version bumped to `1.2.0`
- `split_files.py` version bumped to `1.2.0`
- CI workflow: added `textblob` and `pydantic` to install step, extended mypy scope

## [Unreleased]

### Added
- `utils/` package with `logger.py` (structured logging) and `validators.py` (input validation)
- `scripts/validate_jsonl.py` CLI tool for JSONL file validation
- `scripts/benchmark.py` for measuring split performance
- `tests/conftest.py` with shared fixtures (`sample_jsonl`, `tiny_jsonl`, `large_jsonl`, `full_env`)
- `tests/test_data_quality.py` — JSON output correctness tests
- `tests/test_integration.py` — end-to-end CLI integration tests
- `tests/test_validators.py` — tests for validation helpers
- `tests/test_logger.py` — tests for logging configuration
- `tests/test_snowflake_helpers.py` — tests for execute_query, health_check, managed_connection
- Parametrized edge-case tests in `test_split_files.py` and `test_snowflake_connector.py`
- `Makefile` with `install`, `test`, `lint`, `type-check`, `format`, `clean` targets
- `.pre-commit-config.yaml` with ruff, trailing-whitespace, mypy hooks
- CI matrix for Python 3.10, 3.11, 3.12 with coverage artifacts
- `CONTRIBUTING.md` development guide

### Changed
- `snowflake_connector.py`: added `execute_query()`, `health_check()`, `managed_connection()`, `get_connection_iterator()`, and `SNOWFLAKE_ROLE` env var support

## [1.0.0] — 2025-01-01

### Added
- Initial release
- `split_files.py` — CLI tool to split large JSONL files into chunks
- `snowflake_connector.py` — Snowflake connection utility with env-var validation
- `UDF and tables.sql` — Snowflake UDF and table definitions
- `Dockerfile` — container build configuration
- `README.md` — project documentation
- `SECURITY.md` — security policy
- GitHub Actions CI workflow
- `pyproject.toml` with ruff and pytest configuration
