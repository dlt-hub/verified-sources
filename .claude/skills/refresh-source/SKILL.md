---
name: refresh-source
description: Create a new verified source or refresh an existing one against the current state of its upstream API/SDK - find breakages, outdated patterns and data access gaps (large volumes, bulk export, Arrow, upfront schema, incremental/merge setup), review changes to the loaded data, and propose (or implement) a plan with credential-free unit tests. Use for new source requests, when users report a source is outdated or broken, or for periodic source maintenance.
argument-hint: <source-name> -- [optional hints what to pay attention to]
---

# Create or refresh a verified source

Arguments: `$ARGUMENTS` — first token is the source folder name under `sources/`, anything after `--` is user hints (specific user reports, suspected areas, the data needed).

Work through the phases in order. Do not skip phase 0 — wrong conclusions are usually formed by analyzing code that is no longer alive or pins that no longer reflect reality.

**New source**: when `sources/<name>/` does not exist, skip the steps about existing code, pins and dead code, and follow "Walkthrough: Create and contribute a new source" in `CONTRIBUTING.md` and the [new source checklist](../review-pr/new-source-checklist.md). Start the source at version `1.0.0` with the README described in [Source version and README](#source-version-and-readme).

## Phase 0 — Ground truth

0. Build the context with [create-source-context](../create-source-context/SKILL.md): the data the upstream offers, authoritative docs, existing connectors, auth, pagination, limits and a data inventory. For a new source, stop after it until the user confirms the scope.
1. Read everything in `sources/<name>/`, `sources/<name>_pipeline.py`, `tests/<name>/`. Start with `README.md` and `version.py`: the changelog, extension points and operations notes of the source.
2. `git log --oneline -- sources/<name>` — look for past removals/refactors. **Hunt dead code**: files, fixtures, conftest helpers, docstrings and README claims orphaned by earlier PRs. Dead code should be deleted, never "fixed". Dead code may also be the only reason for a heavy dependency — deleting it can drop whole packages from `requirements.txt`.
3. Compare the THREE dependency declarations and flag mismatches:
   - `sources/<name>/requirements.txt` — what `dlt init` users actually install (often unbounded!)
   - `pyproject.toml` dependency group `<name>` — what CI tests
   - any version pinned in code (e.g. `api_version = "..."`)
   A CI group pinned years behind an unbounded requirements.txt means CI is testing a different client than users get — that is how breakage ships silently.
4. Check open/closed GitHub issues for the source (`gh search issues`) and any user hints from the arguments.

## Phase 1 — Research the upstream (web search, authoritative sources only)

For a new source, `create-source-context` covers this phase.

Use WebSearch/WebFetch against official docs, SDK changelogs, and SDK migration guides (vendor docs site, vendor GitHub wiki/CHANGELOG). For each finding record the source URL.

- Current SDK major version and every breaking change between the version CI pins and today (migration guides are gold: object model changes, removed dict-inheritance, auth patterns, deprecated module-level clients).
- Current API version / versioning model; what the code pins vs. what is current; breaking changes in between (renamed/removed/restructured fields the source or its helpers depend on).
- Official request/response shapes the code touches: list envelopes, pagination cursors, filter parameter formats, retention limits (e.g. event retention windows). Capture documented example payloads — they become test fixtures in phase 4.

## Phase 1b — Data access audit

Breakages are not the only reason to refresh: many sources load correctly but cannot handle real data volumes. Answer each question below for **every resource**, with the doc URL behind each claim. Where docs are silent, say it is a guess and why (e.g. "activity objects are typically the largest tables in a CRM").

1. **Large volumes.** Which endpoints can hold millions of records (activities, events, history/audit logs, line items, messages)? From docs and the web: page size limits, request/export quotas (daily API calls), export file size and count limits, query timeouts. Estimate the cost of a full load (requests = records / page size against the quota) and check how the code behaves at that size:
   - Is the whole response or file materialized in memory, e.g. `response.json()` on a large export file?
   - Is there any checkpointing within a run?
   - Do large mutable tables use `replace`, so they are reloaded in full on every run?
2. **Bulk loading.** Is there a bulk, export or async API (e.g. Salesforce Bulk API 2.0, export jobs, report or data dump endpoints)? Record:
   - its limits and server-side chunking;
   - the objects and fields it does not support, and whether a fallback to the standard API is needed. Detect "not supported" errors at runtime instead of hard-coding object lists;
   - job polling and how much of a run is spent waiting on the job;
   - whether a job can outlive the auth session. The session must be renewed with a lock, so clients shared across parallel resources log in only once, and the job resumed from the same page;
   - whether the SDK retries 429 and 5xx. Most don't: pass dlt's `Client(raise_for_status=False).session` so the SDK still maps errors.

   **Check other popular open source connectors** for the same API: which API they use for which objects and fields, when they fall back, and why. Take the facts, not the code, because their licenses may not allow copying.

3. **Arrow backend.** Can the response be parsed straight into Arrow (CSV via `pyarrow.csv`, NDJSON, Parquet or Arrow exports), with types applied by dlt's own mapper (`dlt.common.libs.pyarrow.cast_arrow_as_columns_schema` / `columns_to_arrow`, using `caps` from the `Container`)? Requirements:
   - `pyarrow` stays **optional**: a `backend` option ("auto" picks pyarrow when installed via `importlib.util.find_spec`, without importing it), and a "python" backend that parses with the standard library and never imports pyarrow. Prove it with a test that sets `sys.modules["pyarrow"] = None`.
   - Both backends must load identical values and types; compare them on captured data.
   - Known traps:
     - pyarrow's default `null_values` turns the strings `NA`, `N/A` and `NULL` into nulls;
     - a page with only a header gives a 0-row table that dlt's cast rejects;
     - null bytes;
     - whole numbers written as `42.0`.
   - Measure throughput offline with captured data, using the local `filesystem` destination with `jsonl` so the destination's load step doesn't skew the numbers (duckdb `insert_values` dominates otherwise).
4. **Schema upfront.** Is there a metadata endpoint (describe, OpenAPI, JSON schema) to compute columns before loading, like `sql_table` reflection?
   - Map vendor types to dlt types: precision and scale for decimals, `timezone` for timestamps, `date`, `time`, `json`. Skip fields the API cannot return. Unknown types become `text`.
   - Pass the result as resource `columns` hints and let user `apply_hints` override it.
   - Verify the mapping against **real** responses: vendors store values differently than written (numbers as doubles, trimmed whitespace, second-precision datetimes, sanitized HTML).
   - Flag options users may want, e.g. floats as decimals.
5. **Incremental and merge.** For each resource:
   - **Cursor:** is there a server-side filterable, preferably indexed, "updated at" field? Prefer the one that changes on every update, e.g. `SystemModstamp` over `LastModifiedDate`.
   - **Primary key:** is it set? Merge without a key silently appends.
   - **Disposition:** does it match mutability: merge for mutable records, append for insert-only (history, logs), replace only without a cursor?
   - **Query:** are queries built from the incremental, with `incremental.get_current_range()` and a closed range so boundary rows are deduplicated by the primary key, which dlt passes to the incremental?
   - **Lag:** is there a lag for records committed late?
   - **Deletes:** do they propagate (soft-delete flag as `hard_delete`, deleted-records endpoints)?
   - **Ordering:** does ORDER BY or LIMIT defeat server-side chunking?
   - **Tests:** plan the [standard incremental tests](#standard-incremental-tests) for each incremental resource.
   - **Defaults:** give mutable resources a default incremental on the "updated at" cursor, with lag and `dedup_sort="desc"` on the cursor, and a `start_date: Optional[TAnyDateTime]` argument as its initial value (all records on the first run if not set). Accept your own `incremental` instead, used as is, and raise if both are passed. Do not overload one argument with sentinels like `"auto"`. dlt resolves every argument typed as an incremental from config and merges config into a passed one, `NotResolved` does not stop it.
6. **Network compression.** Is the data compressed in transfer?
   - **Where to look:** `Accept-Encoding: gzip` on HTTP APIs and export downloads, the wire compression of database drivers, e.g. MongoDB `compressors`, and compressed export formats.
   - **Local vs. remote:** measure both.
     - A remote upstream is bandwidth bound. MongoDB Atlas sends BSON uncompressed unless the client asks for compression, and zlib took a load from 78 s to 30 s.
     - A local or same-network upstream is CPU bound, and the same zlib made it about 30% slower.
     - Report both numbers.
   - **Default:** check what other connectors of the same upstream do, e.g. if they compress by default.
     - Most installs reach a remote upstream, so enable it by default.
     - Let the user's connection string or option override the default, including turning it off. Check precedence: pymongo keyword arguments override the URL.
     - Prefer a codec from the standard library, e.g. zlib. Offer faster ones, such as zstd and snappy, only when their package is installed, and silence the driver's warnings about missing ones.

## Source structure and dlt building blocks

Build the source like an SDK that is easy to extend. No black boxes:
- **Layers**: separate the connection (login, session renewal, retries), the API clients, data conversion and the dlt resources. Clients for different APIs share one interface, e.g. Bulk and REST query clients that yield the same pages. Each layer is usable and testable on its own, so users can run their own queries with the source's clients and helpers.
- **One resource factory** that loads any object, like `sql_table`; the source composes it.
- **Everything visible and configurable**: no global state, no hidden lists where the API can tell (describe endpoints, errors), options as arguments and config, hints users can change with `apply_hints`.

Use dlt building blocks instead of hand-rolled code. Find dlt docs pages in the [LLM index](https://dlthub.com/docs/llms.txt); every page is served as markdown at its `.md` URL:
- standard REST APIs: the declarative [`rest_api` source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic.md);
- custom REST code: [`RESTClient`](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/advanced.md#restclient) with its [paginators](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/advanced.md#paginators) and [authentication](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/advanced.md#authentication). Write a custom paginator or auth class rather than a loop;
- other HTTP calls and SDKs: the [requests wrapper](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/advanced.md#requests-wrapper) with retries, passed to the SDK as its session;
- [incremental loading](https://dlthub.com/docs/general-usage/incremental-loading.md) translated to API filters ([cursor](https://dlthub.com/docs/general-usage/incremental/cursor.md));
- [resources and hints](https://dlthub.com/docs/general-usage/resource.md) and [credentials specs](https://dlthub.com/docs/general-usage/credentials/complex_types.md);
- tables with schema upfront, backends and deferred reflection: follow [`sql_table`](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/advanced.md).

When a building block does not fit, e.g. CSV responses that `RESTClient.paginate()` cannot parse or login flows only an SDK implements, say why in the plan.

## Phase 2 — Reproduce empirically. Never claim a breakage you haven't run.

Use ephemeral environments to prove each suspected breakage and each proposed fix, at **both ends** of the allowed dependency range:

```sh
cd /tmp && uv run --with "<sdk>==<oldest-allowed>" python - <<'EOF' ... EOF
cd /tmp && uv run --with "<sdk>==<newest>" python - <<'EOF' ... EOF
```

Construct real SDK objects (e.g. `construct_from`-style factories) and exercise the exact code path that is suspected broken. Subtle version differences matter (e.g. a conversion helper being shallow in one major and recursive in another) — only execution reveals them.

## Phase 3 — Plan (present before implementing)

Tier the findings; keep tiers separable:

- **P0 — unbreak**: minimal fixes that work across the whole supported dependency range; align CI group with what fresh users install; fix requirements bounds (add upper bound to majors not yet verified). Must NOT change loaded table schemas.
- **P1 — modernize client**: new SDK client patterns, drop global state, configurable API version. Anything that changes response shapes **changes loaded schemas for existing users** — call this out explicitly as breaking and keep it out of P0.
- **P2 — features**: new endpoints, incremental improvements, and the data access findings of phase 1b: bulk API, `backend` option, upfront schema, default incremental with `start_date`. These usually change loaded schemas, so they are **breaking**: document the migration (e.g. `refresh="drop_sources"`).

For a new source, the plan lists the resources from the data inventory, the structure (see [Source structure and dlt building blocks](#source-structure-and-dlt-building-blocks)), write dispositions and tests instead of tiers.

Classify every planned change with [Data shape and upgrade compatibility](#data-shape-and-upgrade-compatibility) before presenting the plan, and name the version bump it leads to (see [Source version and README](#source-version-and-readme)).

Prefer dlt built-ins over hand-rolled code (see [Source structure and dlt building blocks](#source-structure-and-dlt-building-blocks), and `dlt.common.time.ensure_datetime_in_tz` / `ensure_date`, the `dlt.common` logger) — but verify the utility exists in the oldest dlt allowed by the source's requirements.txt before using it, and bump that minimum when needed. Do not add new `pendulum` code: use stdlib `datetime` with dlt's helpers.

## Data shape and upgrade compatibility

The most important part of any change to a source: users upgrade a pipeline that already loaded data and stored state. Any change that stops that pipeline from loading, or makes it load wrong data, is **breaking**. Answer the question of every user: can I upgrade my pipeline, does it keep working, and do I lose data? Understand the data before and after the change, and warn the user about every difference.

**1. Inventory the shape** of every resource, before and after:
- tables: names, nested (child) tables created from lists;
- columns: names, data types with precision, scale and timezone, nullability, variant columns, the format of values, e.g. of dates or arrays stored as text;
- hints: primary and merge keys, write disposition, `hard_delete` and other column hints;
- incremental: cursor field, the type of its values, the state key (resource name and cursor path), initial value, `start_date` default, lag, deduplication key;
- change positions kept in state: CDC positions (replication slots, LSNs), change tokens, page cursors;
- requirements: minimum dlt and package versions, arguments, config keys and secrets.

**2. Observe the upgrade on a live account**, which is better than reasoning about it. Run the old version (a worktree from `origin/master`) and then the new one with the same pipeline name, the same dataset and the same destination, as users do. Use a disposable dataset. Change upstream records between the runs. Compare:
- the schemas, e.g. `dlt pipeline <name> schema` after each run;
- the incremental state in `pipeline.state`, and whether the first run after the upgrade continues from it: no records missed, none loaded again;
- the loaded rows: new `__v_` variant columns, duplicates, missing rows, failed load jobs, values in a new format next to old ones in the same column;
- the upgrade path you recommend: run it and check that it ends with the new shape and no missing rows.

Without an account, run both versions on captured responses (phase 4) and compare the same.

**3. Classify each change.** Breaking:
- column type changes the destination cannot coerce: dlt creates variant columns or the load fails;
- new NOT NULL columns or keys, or a stricter nullability of existing data;
- changed primary or merge keys or write disposition: merge stops matching old rows, rows are duplicated or not deleted;
- renamed resources or tables: a new empty table, the state starts over and a full reload follows;
- a different cursor field or cursor value type: stored `last_value` cannot be compared with new data (e.g. a string state compared with an Arrow timestamp column), or queries start from a wrong value;
- a different deduplication key: boundary records are loaded twice;
- a gap or an overlap in incremental loading: the first run after the upgrade starts from another position, because of a changed state key, initial value, `start_date` default or lag, or a CDC position that is not carried over. Records are missed, or loaded again beyond what merge absorbs;
- a new format of values in an existing column of the same type, e.g. arrays as JSON instead of Postgres literals: old and new values mix in one column, with no variant column and no failed load;
- changed handling of deletes and key updates: rows remain in or disappear from the destination, unlike in earlier loads;
- removed or renamed arguments and config keys, new required secrets: the pipeline fails before it loads;
- data format switches that change dlt columns, e.g. JSON rows to Arrow tables needs `[normalize.parquet_normalizer] add_dlt_load_id` and `add_dlt_id` for tables created from rows.

Not breaking, but report them: new columns or tables, dropped columns (left in the destination with nulls), type widening the destination coerces, a higher minimum dlt or package version, fixed bugs whose wrong rows stay in the destination until a reload.

**4. Warn the user** in the plan, the PR description, the README changelog and the dlt docs (phase 6): answer whether existing pipelines can upgrade, list every change, mark the breaking ones with what fails, and give the upgrade path, e.g. `pipeline.run(source, refresh="drop_sources")`, `dlt pipeline <name> drop <resource>`, a config option or an option that keeps the old behavior.

**5. Test it**: run the pipeline twice so the state carries over between runs, and test switches users can make between runs (e.g. a backend picked automatically when a package gets installed).

## Source version and README

**Every change to a source that goes into a PR bumps its version and updates its README, in the same PR.** A patch version may skip the changelog entry. `dlt init` copies both files into user projects, and the copies carry no git history.

`make lint` enforces it with `tools/check_source_versions.py`: each source with changes to files other than its README, compared with the merge base of `master`, needs a `version.py` higher than on `master` and, unless it is a patch version, a README with a changelog heading for that version, e.g. `### 1.1.0`.

`sources/<name>/version.py` holds the version of the source. Add it with `1.0.0` when it is missing:

```py
__version__ = "1.0.0"
```

The version follows the [semantic versioning of dlt](https://github.com/dlt-hub/dlt#adding-as-dependency):
- `major`: breaking changes, see [Data shape and upgrade compatibility](#data-shape-and-upgrade-compatibility), and removed deprecations;
- `minor`: new features, e.g. resources and options, sometimes automatic migrations;
- `patch`: bug fixes.

`sources/<name>/README.md` is the maintenance document of the source, for developers and coding agents that change it. It holds only what is not easily read from the code: no defaults, page sizes, type mappings, test helpers or descriptions of how a resource works. Setup and usage belong in the dlt docs (phase 6). The README holds:
- **Changelog**: an entry per version, newest first. It answers the question of a user whose pipeline already loaded data: can I upgrade, and do I lose data? The entry starts with an **Upgrade** line, from what [Data shape and upgrade compatibility](#data-shape-and-upgrade-compatibility) found: `safe` when existing pipelines continue with no action, otherwise the steps in order and what happens without them, i.e. failed runs, missed, duplicated or mixed rows. It says whether incremental state carries over. Short bullets follow: start each with **breaking**, new, changed or fixed and name the resource whose data changes. Breaking bullets say what fails;
- **Extension points**: how to add e.g. an endpoint, a field type or a transformer;
- **Seeding**: numbered steps to seed the test account and capture responses;
- **Tests**: only what the test code does not show, e.g. tests that write to the test account. Not what mocks, fixtures and helpers do;
- **Operations**: what running against the upstream taught: run times and request costs, throttling and quotas, paging quirks, troubleshooting. Name the source of each fact: a run on the test account, a doc with its URL, or an earlier PR. A page size or limit stays only when exploring the upstream set it, stated as the upstream limit with its source, e.g. "search serves at most 5000 ids per page (docs)".

```md
## Changelog

### 2.0.0
**Upgrade**: run `dlt pipeline <name> drop order_items`, the next run loads it again with `price`
as `decimal`. Without it the existing column stays `double`. `orders` loads into the new
`sales_orders` from `start_date`, drop the `orders` table afterwards. The incremental state of
`invoices` carries over.
- **breaking** `orders`: renamed to `sales_orders`, with a new table and new incremental state
- **breaking** `order_items`: `price` is `decimal` instead of `double`
- new resource `refunds`

### 1.1.0
**Upgrade**: safe, incremental state carries over.
- new resource `invoices`
```

## Phase 4 — Tests (the biggest pain point)

Existing source tests usually need upstream credentials and never run for contributors. Always add **credential-free unit tests**:

- In modules that follow the source modules, see [Test modules](../../rules/testing.md#test-modules).
- **Use proven shapes, not invented ones**: fixtures must mirror request/response shapes, pagination envelopes and cursor semantics found in official docs during phase 1 (cite which doc page a fixture mirrors when it isn't obvious).
- Mock at the transport/SDK boundary with **real SDK objects** (`construct_from` etc.), monkeypatching the API call — so the installed SDK's serialization actually runs instead of a MagicMock lying about it.
- Cover: pagination control flow (cursor passed from last item, stop on terminal page flag), parameter construction (filters, special-cased endpoints), and any date/type conversion helpers with one case per accepted input type.
- Run the unit tests against both ends of the SDK range (phase 2 envs) before declaring the range supported.
- **Pass resolved credentials** (`credentials.resolve()`) to resources in unit tests, otherwise dlt replaces them with real ones from the secrets.
- Patch sleeps in polling code. Test session renewal and retries (401, 429, 5xx) at the transport level with `requests_mock`.

### Test accounts and captured data

Hand-made fixtures encode our assumptions; they hid the header-only page bug in the Salesforce refresh. When the source has a test account:
- **Seed it as code** under `tests/<name>/seed/`: one re-runnable, idempotent script that creates metadata (custom objects or fields covering every field type) and records with edge-case values. Upsert on external ids, never delete and re-create. The records are:
  - null-like and number-like text;
  - quotes, line breaks and unicode;
  - boundary numbers and dates;
  - blank values;
  - deleted records.
- **Capture real responses** from the seeded account into `tests/<name>/cases/`. Use only synthetic seed data, never customer data. Run credential-free unit tests on them, compared field by field with expected values derived from the seed data.
- **Keep end-to-end tests read-only, few and parallel**, in `test_<name>_source.py`. Count only seeded records, because accounts come with their own sample data. Pause between live runs, since vendors throttle.
- **Never read or print secrets files.** Credentials reach the seed script and tests only through dlt config injection.
- **Check other popular open source connectors** for the same API and their test suites for edge cases not covered yet: value formats, nulls, quoting, pagination, errors, unsupported objects. Add them as test cases. Take the cases, not the code: write fixtures and values ourselves, because their licenses may not allow copying.

### Standard incremental tests

Every source with incremental loading has these end-to-end tests, on the seeded test account or on a local container (`tests/<name>/docker-compose.yml`). Reference: `test_local_seeded_full_and_incremental` and `test_local_seeded_chunks` in `tests/mongodb/test_mongodb_source.py`.

**Seed**
- Unique random cursor values at the precision of the upstream, in an order unrelated to the insert or primary key order.
- Several chunks and a partial last one, for example 1050 records in chunks of 200.
- Expected values come from the seeded records, never from another read of the upstream.

**Tests**
1. Full load: loads all seeded primary keys.
2. Next run: loads 0 rows.
3. Windows with `initial_value`, `end_value`, `range_start="closed"` and `range_end="open"`: each window loads the seeded values in it and stores no incremental state.
4. Chunks with `add_limit(1)`, `row_order="asc"` and `range_start="open"`: each run loads the next `chunk_size` values, ascending with `max` and descending with `min`, and stores the last one as `last_value`. A final run loads 0 rows. The stored `last_value` keeps the timezone of the cursor.

**Parametrization**
- Every backend and reader: `python` and `pyarrow`, sequential and parallel, Bulk and REST.
- Chunks: `max` and `min`, each with and without `initial_value`.
- A parallel reader that fails the chunk test needs `parallel=False` for chunked loading, written in Operations.

## Phase 5 — Verify and finish

1. `uv lock` if pyproject changed; ensure resolution works on all supported Python versions.
2. Unit tests must pass without anything in `sources/.dlt`.
3. End-to-end tests only if credentials exist; otherwise state plainly they were not run.
   With a test account, run the upgrade from [Data shape and upgrade compatibility](#data-shape-and-upgrade-compatibility) and report what changed.
4. `make lint-code` and `make format-lint`.
5. Bump `version.py` and add the README changelog entry, then update the README sections and docstrings the changes touched (see [Source version and README](#source-version-and-readme)). Report findings with file:line references and the doc URLs that back each claim.

## Phase 6 — Assess the public dlt docs

The user-facing docs live in the separate public `dlt-hub/dlt` repo at
`docs/website/docs/dlt-ecosystem/verified-sources/<name>.md` and are also served to LLMs via
https://dlthub.com/docs/llms.txt — one fix covers both. Fetch the file
(`gh api repos/dlt-hub/dlt/contents/docs/...`) and review it against the refreshed source:

- **Code drift**: constant names, default endpoint tuples, function signatures and links quoted in the docs must match `settings.py`/`__init__.py` exactly — docs commonly lag source-repo PRs (e.g. an endpoint moved between ENDPOINTS tuples).
- **Copy-pasteable examples are the highest-stakes content**: an example demonstrating a removed or unsafe pattern (e.g. incremental loading of an editable endpoint) silently produces wrong data for everyone who pastes it.
- **Document what the refresh established**: supported SDK version range, pinned upstream API version and its schema implications, accepted input formats, retention limits and other upstream constraints discovered in phase 1, plus the phase 1b results: which API is used for which resources, backend options and their throughput, type mapping, default incremental and write dispositions, known limitations, and a migration note for breaking changes.
- Propose the changes as a diff against the fetched file; the actual PR goes to `dlt-hub/dlt`, not this repo.
