# ServiceNow

Maintenance notes of the ServiceNow source for developers and coding agents. The source reads
tables with the [Table API](https://www.servicenow.com/docs/r/api-reference/rest-apis/c_TableAPI.html)
or the CSV export of a list (`<table>_list.do?CSV`). It reads keyset pages ordered by `sys_id`. Field
types come from `sys_dictionary`, and deletes come from `sys_audit_delete`. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/servicenow).

## Changelog

### 1.0.0
**Upgrade**: new source.
- new `servicenow_source` and `servicenow_table`: incremental merge on `sys_updated_on` with deletes,
  `start_date`, your own `max` or `min` incremental on date-time, date, number and text fields, the
  Table API or the CSV export, `python` and `pyarrow` backends, `sys_id` ranges read in parallel,
  and `defer_table_reflect`

## Extension points

- A table of `servicenow_source`: an entry in `DEFAULT_TABLES` of `__init__.py`.
- A field type, in `helpers/schema.py` unless noted:
  - its dlt column: `internal_type_to_column`
  - types that are not loaded: `SKIPPED_TYPES`
  - durations: `DURATION_TYPES`
  - the cursor fields of the default incremental: `UPDATED_AT_FIELD`, `CREATED_AT_FIELD`
  - the conversion of values: `helpers/convert.py`. Both backends must give the same values.
- A reader: a `fetch_page(query, limit, offset)` callable passed to `_keyset_pages` of
  `helpers/client.py`, like `pages`, `arrow_pages` and `csv_pages`. The loop keeps the keyset
  position, halves the page after a cancelled transaction and skips pages hidden by ACLs. It is not a
  `RESTClient` paginator. `RESTClient.paginate()` parses every response with `response.json()`. An
  error raised inside it ends the generator, so a cancelled page cannot be read again with fewer
  records. Requests still go through `RESTClient` and dlt's `Client` session.
- The incremental: `table_resource` of `helpers/table.py` creates the default incremental with its
  lag and without deduplication. `table_rows` builds the queries and reads deletes from the
  incremental that dlt passes, so an incremental set with `apply_hints` replaces the default one.
- Query literals of a field type: `to_glide_literal` and `LITERAL_TYPES` in `helpers/query.py`.
  `incremental_query` turns an incremental into conditions and the order of records.
- Reading fields: `helpers/schema.py`. `reflect_columns` computes the columns when the resource is
  created or, with `DeferredReflect`, when extracting. `LazyDictionary` reads `sys_dictionary` once
  for all resources of a source.
- A retried response: `_should_retry` of `helpers/client.py`, passed to dlt's `Client` in
  `make_session`.
- An auth method: a credentials class in `helpers/credentials.py`, added to `ServiceNowAuth`.
- Parallel reads of a table: `plan_partitions` of `helpers/table.py` and the `sys_id` bounds of
  `partition_conditions` in `helpers/query.py`.

## Seeding

The test instance setup and the seeded tables are described in `tests/servicenow/README.md`. The
seed runs from the repository root and reads credentials from `sources/.dlt`:

1. Run `uv run python -m tests.servicenow.seed.seed deploy` to create the tables and fields.
2. Run `uv run python -m tests.servicenow.seed.seed load` to write the records of `seed/data.py`. It
   also creates and deletes records once, so `sys_audit_delete` has records.
3. Run `uv run python -m tests.servicenow.seed.seed capture` to rewrite `tests/servicenow/cases/`.
   Commit the result.
4. Optional: run `capture-hidden` to capture responses while a field ACL hides a field. Before you
   run it, create the ACL in the UI, see `tests/servicenow/README.md`.

`all` runs the first three steps. A developer instance is reclaimed after 10 days without a login.
On a new instance, run all steps again.

## Tests

`test_servicenow_source.py` only reads the instance. Its expected values come from `seed/data.py`,
except `test_all_resources`, `test_large_table` and `test_partitioned_load_identical`, which read the
demo data of the developer instance. No test covers these cases, which were tried once by hand on the
test instance:
- the API user in the time zones America/Los_Angeles and Hongkong
- field `read`, `query_range` and `query_match` ACLs, which need `security_admin` in the UI

## Operations

Facts seen on the developer instance on the Brazil release (test instance), or taken from docs and
other connectors.

- **Throughput**: the instance checks field ACLs for every value it returns. A page of 10000
  `sys_dictionary` records with 59 fields takes about 38 s, 27 s of it ACL time in
  `syslog_transaction`. With 12 fields, a page takes about 6.5 s (test instance). Benchmarks
  published by other connectors agree: about 200 records/s for wide tables.
  `syslog_transaction` splits the time of each request into SQL, ACL and wait times.
- **Sessions**: the instance runs one transaction per session. Requests that shared the session
  cookie waited 3 to 10 s each (`session_wait_time`), so the session does not keep cookies. A new
  session per request costs about 0.05 s (test instance, and
  [community article](https://www.servicenow.com/community/now-platform-articles/inbound-integrations-performance-design-on-the-servicenow/ta-p/2316967)).
- **Parallel reads**: 12 tables with 97000 records loaded in 35 s with 5 extract workers and in 22 s
  with 10. One table of 152000 records loaded in 162 s page by page, in 58 s in `sys_id` ranges with
  5 workers and in 34 s with 10. Both ways load the same records. `sys_id`s generated by the instance
  are random hex: each first hex digit held 5.8 to 6.8% of 160000 `sys_dictionary` records (test
  instance). A node runs about 4 API transactions at once and queues 50, then answers HTTP 429
  ([semaphores](https://www.servicenow.com/docs/r/impact/io-semaphores-performance-metrics.html)).
  Ranges are not read with `row_order` or `add_limit`, chunked loads read one page after another.
- **Quotas**: Table API transactions are cancelled after 60 s by default
  ([quota rules](https://www.servicenow.com/docs/r/platform-administration/platform-performance/c_DefaultQuotaRules.html)).
  A cancelled transaction returns HTTP 200 with part of the page and an error, or truncated JSON.
  The CSV export ends with a line `Transaction cancelled`. The page is read again with half the records
  (test instance).
- **Urls**: urls longer than about 8192 bytes get an HTML 414 (test instance).
- **ACLs**:
  - A field hidden by a `read` ACL is missing from Table API records. In the CSV export, it is an
    empty, unquoted value, while other empty values are quoted.
  - A query on a field without `query_match` (`=`, `ISEMPTY`) or `query_range` (`>`, `<`) access gets
    HTTP 403 from the Table API and a CSV export without records.
  - `X-Total-Count` and the aggregate API count records hidden by row ACLs: 160520 counted, 160148
    readable in `sys_dictionary`, even for the admin user.
  - Pages are limited before row ACLs, so a page can be shorter than the page size or empty while
    records remain.

  (test instance)
- **CSV export**: no charset in `Content-Type`, Windows-1252 unless `glide.export.csv.charset` is
  UTF-8. All values are quoted, text starting with `=+-@` gets an apostrophe, booleans are `1` and
  `0`, the header is in random order (test instance). An export returns at most 10000 records
  ([export limits](https://www.servicenow.com/docs/r/washingtondc/platform-administration/table-administration-and-data-management/c_ExportLimits.html)),
  so a larger `page_size` is lowered to 10000 with a warning. Without it, a page of 20000 is never
  full, and dlt does not read in `sys_id` ranges.
- **Dates**: date literals in queries compare in UTC, whatever the time zone of the API user. Only
  `javascript:gs.dateGenerate()` uses the time zone of the API user. Empty values sort first in
  ascending and last in descending order, and range conditions do not select them (test instance).
- **Query literals**, checked with read-only queries on the seeded tables (test instance):
  - Integers compare and sort as numbers: `u_number>99` selects 950 of 0..1049, and `ORDERBY`
    puts 9 before 10.
  - `u_date>=2024-02-29` and `u_date>=2024-02-29 00:00:00` select the same records.
  - Decimals, currency, longints above 2^53 and floats written without exponent compare as numbers.
    The instance does not parse exponents. Empty currencies are stored as `0`.
  - `>` works on text, but case-insensitively: `types-n` and `TYPES-N` select the same records,
    while Python compares case-sensitively.
  - A condition on a field that does not exist is ignored, so all records are returned. The cursor
    must be a field in `sys_dictionary`. The dlt docs warn about invalid encoded queries.
- **Deletes**: `glide.ui.audit_deleted_tables` decides which `sys_*` tables audit deletes.
  `sys_choice` is not one of them (test instance).
- **Timestamp ties**: 588 `sys_choice` records share one second and 86446 `sys_dictionary` records
  were written within 10 minutes when the instance was installed (test instance). When more records
  share one timestamp than a chunk holds, a chunked load with `range_start="closed"` reads the same
  chunk again. With `"open"`, it skips the rest of them.
