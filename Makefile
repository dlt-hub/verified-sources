.PHONY: dev lint test

help:
	@echo "make"
	@echo "		dev"
	@echo "			prepares development env"
	@echo "		lint"
	@echo "			runs flake and mypy on all sources"
	@echo "		test"
	@echo "			tests all the components including destinations"

dev:
	uv sync --reinstall-package dlt --upgrade-package dlt

lint-dlt-init:
	uv run ./check-requirements.py
	uv run pytest tests/test_dlt_init.py --no-header

lint-code:
	./check-package.sh
	uv run mypy --config-file mypy.ini ./sources
	# uv run mypy --config-file mypy.ini ./tests
	uv run mypy --config-file mypy.ini ./tools
	uv run flake8 --max-line-length=200 --extend-ignore=W503 sources init --show-source
	uv run flake8 --max-line-length=200 --extend-ignore=W503 tests --show-source
	uv run black ./ --diff

lint: lint-code lint-dlt-init

format:
	uv run black ./

format-lint: format lint

transpile-rules:
	cd ai && \
	uv run rules render cursor && \
	uv run rules render continue && \
	uv run rules render windsurf && \
	uv run rules render claude && \
	uv run rules render copilot && \
	uv run rules render codex && \
	uv run rules render cline && \
	uv run rules render cody && \
	uv run rules render amp

test:
	uv run pytest tests

test-local:
	ALL_DESTINATIONS='["duckdb", "postgres"]' DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data uv run pytest tests
