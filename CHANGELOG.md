# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
