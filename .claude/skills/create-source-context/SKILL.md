---
name: create-source-context
description: Build the context for creating or upgrading a verified source - the data the upstream offers, authoritative API docs, existing dlt and open source connectors, auth, pagination, limits, bulk and metadata APIs. Use at the start of refresh-source, for new source requests and before planning changes to the data a source loads.
argument-hint: <source-name> -- [which data is needed, user reports]
---

# Create source context

Collect what is needed to build or upgrade a verified source. `refresh-source` starts with this skill. Ported from the `find-source` skill of [dlthub-ai-harness](https://github.com/dlt-hub/dlthub-ai-harness/tree/master/workbench/rest-api-pipeline/skills/find-source).

Parse `$ARGUMENTS`: the first token is the source folder under `sources/` or the upstream product, anything after `--` is context: which data is needed, user reports. If it is not clear which data users need, ask.

## Steps

### 1. Classify the request

| Upstream | Core source |
|---|---|
| postgres, mysql, mssql, oracle, any SQL database | `sql_database` |
| REST API with standard pagination and auth | `rest_api` |
| csv, parquet, jsonl files on s3, gcs, azure or locally | `filesystem` |

New verified sources are accepted only when a core source cannot do the job easily (see `README.md` and `CONTRIBUTING.md`). If a core source fits, stop and report it with a sketch of its configuration, e.g. from the [`rest_api` source docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic.md). The [LLM index](https://dlthub.com/docs/llms.txt) of the dlt docs lists all pages, each served as markdown at its `.md` URL. Typical reasons for a verified source: bulk or export APIs, login flows only an SDK implements, schema from metadata endpoints, many related resources, protocols other than REST.

### 2. Find existing connectors

- **This repo**: `ls sources/`, `uv run dlt init --list-sources`, issues and PRs: `gh search issues "<name>" --repo dlt-hub/verified-sources`.
- **dlthub context**: if the `search_dlthub_sources` MCP tool is available, search it with a few keywords, e.g. "salesforce crm". It returns a description and reference links for the next step.
- **Other open source connectors** for the same API: which API they use for which objects, known limitations and bugs, test cases. Take the facts, not the code: their licenses may not allow copying.

### 3. Read the authoritative docs

Search the vendor docs (`<name> API documentation`), official SDK repos and changelogs. Avoid third-party providers, integrators and proxies. Record the URL behind each fact:
- API versions, deprecations and retirement dates;
- authentication methods and the dlt auth class or credentials spec that fits each;
- pagination of each endpoint and the dlt paginator that fits;
- rate limits, daily quotas, page size and payload limits;
- bulk, export or async APIs and what they do not support;
- metadata endpoints that describe the schema (describe calls, OpenAPI specs);
- filters for incremental loading (updated-at fields) and how deletes are exposed;
- the official Python SDK: is it maintained, and does the source need it.

### 4. Inventory the data

For each object or endpoint: the data it holds, expected volume, whether records change after creation, primary key, cursor field, parent and child relations, and the API that can read it. `refresh-source` uses the inventory in its data access audit and in the data shape review.

### 5. Check a live account

If a test account exists or can be created (e.g. a free developer edition), check the documented shapes, nulls, formats and pagination against real responses. Never read or print secrets: credentials reach scripts only through dlt config injection.

### 6. Present the context

- When the user named the exact data or endpoints, report what was found for them.
- When the user described goals only, present the options and their tradeoffs, and discuss.

Summarize:
- the approach: a core source or a verified source, and why;
- the API used for each resource, authentication, limits;
- the data inventory as a table;
- open questions and doc links.

Ask the user to confirm the scope, e.g. which resources come first, before `refresh-source` plans the work.
