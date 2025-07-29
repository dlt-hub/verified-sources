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
	uv run rules render claude && mkdir -p claude && mv CLAUDE.md claude/ && \
	uv run rules render amp && mkdir -p amp && mv AGENT.md amp/ && \
	uv run rules render codex && mkdir -p codex && mv AGENT.md codex/ && \
	uv run rules render cody && mkdir -p cody && cp -r .sourcegraph cody/ && rm -rf .sourcegraph && \
	uv run rules render cline && mkdir -p cline && cp -r .clinerules cline/ && rm -rf .clinerules && \
	uv run rules render cursor && mkdir -p cursor && cp -r .cursor cursor/ && rm -rf .cursor && \
	uv run rules render continue && mkdir -p continue && cp -r .continue continue/ && rm -rf .continue && \
	uv run rules render windsurf && mkdir -p windsurf && cp -r .windsurf windsurf/ && rm -rf .windsurf && \
	uv run rules render copilot && mkdir -p copilot && cp -r .github copilot/ && rm -rf .github

test:
	uv run pytest tests

test-local:
	ALL_DESTINATIONS='["duckdb", "postgres"]' DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data uv run pytest tests
