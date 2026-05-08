.PHONY: install install-dev test lint type-check format run-validate run-benchmark \
        analyze-sentiment generate-report pipeline-demo clean help

PYTHON   := python3
PIP      := pip
RUFF     := ruff
MYPY     := mypy
PYTEST   := pytest
INPUT    ?= yelp_academic_dataset_review.json
OUTPUT   ?= output/
FILE     ?= $(INPUT)

help:
	@echo "Available targets:"
	@echo "  install            Install runtime dependencies"
	@echo "  install-dev        Install dev + test dependencies"
	@echo "  test               Run full test suite with coverage"
	@echo "  lint               Run ruff linter"
	@echo "  type-check         Run mypy type checker"
	@echo "  format             Auto-format with ruff"
	@echo "  run-validate       Validate a JSONL file (FILE=path/to/file.jsonl)"
	@echo "  run-benchmark      Run the split benchmark"
	@echo "  analyze-sentiment  Run sentiment analysis (INPUT=... OUTPUT=...)"
	@echo "  generate-report    Generate processing report (INPUT=...)"
	@echo "  pipeline-demo      Full demo: split → validate → sentiment → report"
	@echo "  clean              Remove generated files"

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

install-dev:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install pytest pytest-cov ruff mypy textblob pydantic

test:
	$(PYTEST) -v --tb=short --cov=. --cov-report=term-missing --cov-fail-under=75

lint:
	$(RUFF) check . --select E,F,W,I --ignore E501

type-check:
	$(MYPY) split_files.py snowflake_connector.py utils/ pipeline/ models/ --ignore-missing-imports

format:
	$(RUFF) check . --select E,F,W,I --ignore E501 --fix
	$(RUFF) check . --select E,F,W,I --ignore E501 --fix --unsafe-fixes

run-validate:
	$(PYTHON) -m scripts.validate_jsonl $(FILE)

run-benchmark:
	$(PYTHON) scripts/benchmark.py --records 50000 --chunks 10

analyze-sentiment:
	$(PYTHON) scripts/analyze_sentiment.py $(INPUT) $(OUTPUT)/enriched.jsonl

generate-report:
	$(PYTHON) scripts/generate_report.py $(INPUT) --output $(OUTPUT)/report.json

pipeline-demo:
	@echo "=== Step 1: Validate input ==="
	$(PYTHON) -m scripts.validate_jsonl $(INPUT) || true
	@echo "=== Step 2: Analyze sentiment ==="
	$(PYTHON) scripts/analyze_sentiment.py $(INPUT) $(OUTPUT)/enriched.jsonl
	@echo "=== Step 3: Generate report ==="
	$(PYTHON) scripts/generate_report.py $(OUTPUT)/enriched.jsonl --output $(OUTPUT)/report.json
	@echo "=== Done. Report: $(OUTPUT)/report.json ==="

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .coverage coverage.xml htmlcov/ dist/ build/ *.egg-info/ output/
