---
name: contributing
description: Guide for contributing to the verified-sources repository
user_invocable: false
---

# Contributing to verified-sources

When working on this repository, follow these guidelines from CONTRIBUTING.md and README.md:

## What is accepted
- Bugfixes and improvements to existing sources
- New sources ONLY if they cannot be easily implemented via REST API, sql_database, filesystem, or vibe coding
- Accepted categories: queues/brokers, SAP/ERP/HEALTH, HR platforms, GraphQL

## Source structure
- Each source is a self-contained folder under `sources/`
- Source folder contains: Python modules with relative imports, `requirements.txt`, optional `README.md`
- Pipeline demo script: `sources/<name>_pipeline.py` (outside the source folder)
- Tests: `tests/<source_name>/`

## Development workflow
1. `make dev` to set up environment
2. Make changes to source code
3. Test with `python <source>_pipeline.py` from `sources/` folder
4. Run tests: `pytest tests/<source_name>`
5. Lint: `make lint-code`
6. Format: `make format-lint`

## PR requirements
1. Must pass linting (mypy, flake8) and formatting (black)
2. Google-style docstrings in all relevant sections
3. Code must be typed
4. Source folder must contain `requirements.txt` with versioned dlt dependency
5. Use relative imports in source modules
6. Tests must pass on duckdb and postgres
7. Add or update tests for significant changes

## Dependencies
- Add source-specific deps with `uv add --group <source_name> <package>`
- Also add to `requirements.txt` in the source folder
- dlt dependency in requirements.txt: `dlt>=X.Y.Z,<X.Y+1.0`

## Secrets and config
- All sources share config/credentials from `sources/.dlt/`
- Use `sources.<source_name>` sections in secrets.toml
