.PHONY: install-poetry has-poetry dev lint test
.SILENT:has-poetry

help:
	@echo "make"
	@echo "		install-poetry"
	@echo "			installs newest poetry version"
	@echo "		dev"
	@echo "			prepares development env"
	@echo "		lint"
	@echo "			runs flake and mypy on all sources"
	@echo "		test"
	@echo "			tests all the components including destinations"

install-poetry:
ifneq ($(VIRTUAL_ENV),)
	$(error you cannot be under virtual environment $(VIRTUAL_ENV))
endif
	curl -sSL https://install.python-poetry.org | python3 -

has-poetry:
	poetry --version

dev: has-poetry
	poetry install --without unstructured_data

lint-dlt-init:
	poetry run ./check-requirements.py
	poetry run pytest tests/test_dlt_init.py --no-header

lint-code:
	./check-package.sh
	poetry run mypy --config-file mypy.ini ./sources
	poetry run mypy --config-file mypy.ini ./tools
	poetry run flake8 --max-line-length=200 --extend-ignore=W503 sources init --show-source
	poetry run flake8 --max-line-length=200 --extend-ignore=W503 tests --show-source
	poetry run black ./ --diff

lint: lint-code lint-dlt-init

format:
	poetry run black ./

format-lint: format lint

test:
	poetry run pytest tests

test-local:
	ALL_DESTINATIONS='["duckdb", "postgres"]' DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data poetry run pytest tests
