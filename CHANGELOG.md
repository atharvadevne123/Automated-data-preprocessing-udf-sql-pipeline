# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
