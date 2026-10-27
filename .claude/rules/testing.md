---
paths:
  - "tests/**/*.py"
---

# Testing

## Style and structure
- Module-based tests (plain `def test_*() -> None:` functions), not class-based
- pytest with fixtures in `conftest.py`, not unittest setUp/tearDown
- Tests live in `tests/<source_name>/` mirroring the source structure

## Environment and fixtures
Tests run within fixtures (see `conftest.py`) that:
1. Load secrets and config from `sources/.dlt`
2. Set working directory to `_storage` folder (cleaned before each test)
3. Drop destination datasets after each test
4. Restore original environment variables

## Reduce test duplication
- Use `@pytest.mark.parametrize` with human-readable ids
- It is OK to test many related use cases in one test when they share setup

## Destination testing
- Use `ALL_DESTINATIONS` from `tests/utils.py` for parametrized destination tests
- For local development, limit destinations: `ALL_DESTINATIONS='["duckdb"]' pytest tests/<source>`
- `make test-local` runs all tests on duckdb and postgres

## Parallel safety and isolation
- Tests may run in parallel -- use `dev_mode` when creating pipelines
- Use unique pipeline names to avoid collisions
- Clean up fixtures (drop schemas, delete accounts)

## Test helpers
- Common helpers are in `tests/utils.py` (load_table_counts, ALL_DESTINATIONS, etc.)
- Source-specific helpers go in `tests/<source_name>/utils.py`
- Avoid adding source-specific code to the shared `tests/utils.py`

## Running tests
```sh
pytest tests/<source_name>                    # run tests for a specific source
make test-local                               # run all tests on duckdb + postgres
make lint-code                                # mypy + flake8 + black --diff
```

## Linting and formatting
- `make lint-code` runs: mypy, flake8 (max-line-length=200), black --diff
- `make format-lint` formats with black first, then lints
- Both must pass before PR
