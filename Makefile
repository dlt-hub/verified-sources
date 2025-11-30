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
	# export lockfile in widely recognized format
	uv export --format requirements-txt --all-extras --all-groups --no-editable --locked -q --output-file tools/dependabot_lock/requirements.txt

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
	uv run rules render cody && mkdir -p cody && cp -r .sourcegraph cody/ && rm -rf .sourcegraph && cp .rules/.message cody/ && \
	uv run rules render cline && mkdir -p cline && cp -r .clinerules cline/ && rm -rf .clinerules && cp .rules/.message cline/ && \
	uv run rules render cursor && mkdir -p cursor && cp -r .cursor cursor/ && rm -rf .cursor && cp .rules/.message cursor/ && \
	uv run rules render continue && mkdir -p continue && cp -r .continue continue/ && rm -rf .continue && cp .rules/.message continue/ && \
	uv run rules render windsurf && mkdir -p windsurf && cp -r .windsurf windsurf/ && rm -rf .windsurf && cp .rules/.message windsurf/ && \
	uv run rules render copilot && mkdir -p copilot && cp -r .github copilot/ && rm -rf .github && cp .rules/.message copilot/ && \
	sed -i.bak -E 's/^alwaysApply:[[:space:]]*false/alwaysApply: true/' .rules/*.md && rm -f .rules/*.md.bak && \
	uv run rules render claude && mkdir -p claude && mv CLAUDE.md claude/ && cp .rules/.message claude/ && \
	uv run rules render amp && mkdir -p amp && mv AGENT.md amp/ && cp .rules/.message amp/ && \
	uv run rules render codex && mkdir -p codex && mv AGENT.md codex/ && cp .rules/.message codex/ && \
	sed -i.bak -E 's/^alwaysApply:[[:space:]]*true/alwaysApply: false/' .rules/*.md && rm -f .rules/*.md.bak

test:
	uv run pytest tests

test-local:
	ALL_DESTINATIONS='["duckdb", "postgres"]' DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data uv run pytest tests
