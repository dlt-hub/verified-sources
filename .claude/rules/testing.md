---
paths:
  - "tests/**/*.py"
---

# Testing

## Style and structure
- Module-based tests (plain `def test_*() -> None:` functions), not class-based
- pytest with fixtures in `conftest.py`, not unittest setUp/tearDown

## Test modules
- Unit tests: no credentials, no network, upstream mocked, local destinations only. Modules follow
  the source modules: `helpers/client.py` -> `test_client.py`, `__init__.py` -> `test_<name>.py`
- End-to-end tests read from the real upstream or write to a non-local destination. They go only to
  `tests/<source_name>/test_<source_name>_source.py`

## Environment and fixtures
Tests run within fixtures (see `conftest.py`) that:
1. Load secrets and config from `sources/.dlt`
2. Set working directory to `_storage` folder (cleaned before each test)
3. Drop destination datasets after each test
4. Restore original environment variables

## Test helpers
- Always extract shared setup: fixtures to `tests/<source_name>/conftest.py`, builders and mocks to
  `tests/<source_name>/utils.py`
- Common helpers are in `tests/utils.py` (load_table_counts, ALL_DESTINATIONS, etc.)
- Avoid adding source-specific code to the shared `tests/utils.py`

## Reduce test duplication
- Use `@pytest.mark.parametrize` with human-readable ids
- It is OK to test many related use cases in one test when they share setup

## Destination testing
- Tests on a non-local destination, or on a local one run in CI with other destination tests, use
  `@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)`. Never hard-code a destination
- For local development, limit destinations: `ALL_DESTINATIONS='["duckdb"]' pytest tests/<source>`
- `make test-local` runs all tests on duckdb and postgres

## Parallel safety and isolation
- Tests may run in parallel -- use `dev_mode` when creating pipelines
- Use unique pipeline names to avoid collisions
- Clean up fixtures (drop schemas, delete accounts)

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
