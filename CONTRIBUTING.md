# Contributing

Thank you for considering a contribution to this project!

## Getting Started

1. Fork the repository and clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Automated-data-preprocessing-udf-sql-pipeline.git
   cd Automated-data-preprocessing-udf-sql-pipeline
   ```

2. Install dependencies:
   ```bash
   make install
   ```

3. Install pre-commit hooks:
   ```bash
   pip install pre-commit
   pre-commit install
   ```

## Development Workflow

- Create a branch for your change: `git checkout -b feat/my-feature`
- Make your changes and write tests
- Run linting: `make lint`
- Run type checks: `make type-check`
- Run the test suite: `make test`
- Commit your changes following the [Conventional Commits](https://www.conventionalcommits.org/) spec
- Open a pull request against `main`

## Code Style

- Python code is formatted and linted with [ruff](https://github.com/astral-sh/ruff).
- Type annotations are required for all public functions.
- New features must include tests; coverage must not drop below 75%.
- Use Google-style docstrings.

## Running Tests

```bash
make test
# or directly:
pytest -v --tb=short --cov=. --cov-report=term-missing
```

## Commit Message Format

```
type(scope): short description

- feat: new feature
- fix: bug fix
- test: adding or updating tests
- docs: documentation only
- chore: build/config/tooling
- refactor: code restructure without behaviour change
- ci: CI/CD changes
```

## Reporting Issues

Open an issue on GitHub describing the bug or feature request. Please include:
- Python version and OS
- Steps to reproduce (for bugs)
- Expected vs actual behaviour
