# New source review checklist

Use this checklist when reviewing a PR that adds a new verified source. These items are in addition to the standard review steps.

## 1. Acceptance

- New sources are accepted only when they cannot be easily implemented with `rest_api`, `sql_database`, `filesystem` or vibe coding (see `README.md`). Good fits: queues and brokers, SAP/ERP/health systems, HR platforms, GraphQL APIs.
- A plain REST API with standard auth and pagination is a likely reject — recommend a `rest_api` configuration instead.
- Check whether the source was announced in an issue first and whether the dlt team agreed to it.

## 2. Distribution with `dlt init`

- Source folder `sources/<name>/` with `__init__.py`, relative imports only, self-contained (nothing imported from other sources or from `tests`).
- `sources/<name>/requirements.txt` with `dlt` (version range, no extras) and every third-party package the source imports.
- Matching `[dependency-groups].<name>` in `pyproject.toml`, listed in `[tool.uv].default-groups` so CI installs it, and an updated `uv.lock`.
- Demo script `sources/<name>_pipeline.py` with absolute imports from the source package, showing the most common use cases.
- Config and secrets are injected via `dlt.config.value` / `dlt.secrets.value` so `dlt init` can generate the config sections. Secrets use the `sources.<name>` section.
- No real credentials committed anywhere, including `sources/.dlt/config.toml` and test files.

## 3. Tests

- Tests in `tests/<name>/` with an `__init__.py`.
- `tests/test_dlt_init.py` passes for the new source (runs as part of `make lint`).
- Integration tests need a test account. Check whether the dlt team has (or can get) credentials for CI; otherwise those tests will never run.
- Credential-free unit tests cover parsing, pagination and type conversion with realistic payloads.

## 4. Code

- Typed (mypy passes) and Google style docstrings on the source and resource functions.
- Resources declare primary keys, write dispositions and incremental cursors where the upstream data supports them.

## 5. Documentation

- `sources/<name>/README.md` is super short: what the source does and a link to the docs.
- The user docs page is a separate PR to `dlt-hub/dlt` (`docs/website/docs/dlt-ecosystem/verified-sources/<name>.md`, plus its sidebar entry). Flag it as required if missing.
