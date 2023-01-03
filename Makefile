.PHONY: install-poetry has-poetry dev lint test
.SILENT:has-poetry

help:
	@echo "make"
	@echo "		install-poetry"
	@echo "			installs newest poetry version"
	@echo "		dev"
	@echo "			prepares development env"
	@echo "		lint"
	@echo "			runs flake and mypy on pipelines that are typed"
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
	poetry install

lint:
	./check-package.sh
	poetry run mypy --config-file mypy.ini pipelines/chess
	poetry run flake8 --max-line-length=200 pipelines
	poetry run flake8 --max-line-length=200 tests
