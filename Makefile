.PHONY: install test lint type-check format run-validate run-benchmark clean help

PYTHON := python3
PIP := pip
RUFF := ruff
MYPY := mypy
PYTEST := pytest

help:
	@echo "Available targets:"
	@echo "  install        Install project dependencies"
	@echo "  test           Run tests with coverage"
	@echo "  lint           Run ruff linter"
	@echo "  type-check     Run mypy type checker"
	@echo "  format         Auto-format with ruff"
	@echo "  run-validate   Validate a JSONL file (FILE=path/to/file.jsonl)"
	@echo "  run-benchmark  Run the split benchmark"
	@echo "  clean          Remove generated files"

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install pytest pytest-cov ruff mypy

test:
	$(PYTEST) -v --tb=short --cov=. --cov-report=term-missing --cov-fail-under=75

lint:
	$(RUFF) check . --select E,F,W,I --ignore E501

type-check:
	$(MYPY) split_files.py snowflake_connector.py utils/ --ignore-missing-imports

format:
	$(RUFF) check . --select E,F,W,I --ignore E501 --fix
	$(RUFF) check . --select E,F,W,I --ignore E501 --fix --unsafe-fixes

run-validate:
	$(PYTHON) -m scripts.validate_jsonl $(FILE)

run-benchmark:
	$(PYTHON) scripts/benchmark.py --records 50000 --chunks 10

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .coverage coverage.xml htmlcov/ dist/ build/ *.egg-info/
