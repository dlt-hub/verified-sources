# ServiceNow

Maintenance notes of the ServiceNow source for developers and coding agents. The source reads
tables with the [Table API](https://www.servicenow.com/docs/r/api-reference/rest-apis/c_TableAPI.html)
or the CSV export of a list (`<table>_list.do?CSV`). It reads keyset pages ordered by `sys_id`. Field
types come from `sys_dictionary`, and deletes come from `sys_audit_delete`. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/servicenow).

## Changelog

### 1.1.0
**Upgrade**: remove `incremental=None` from `servicenow_table` to keep the default incremental:
with it, each run reads all records and replaces the table. The incremental state carries over.
- changed `servicenow_table`: `incremental=None` loads without incremental, the default
  incremental is the default value of `incremental`
- new `servicenow_table`: an incremental without `cursor_path` sets values of the default
  incremental, e.g. `lag` or `end_value`, also in config. `start_date` wins over its
  `initial_value`

### 1.0.0
**Upgrade**: new source.
- new `servicenow_source` and `servicenow_table`: incremental merge on `sys_updated_on` with deletes,
  `start_date`, your own `max` or `min` incremental on date-time, date, number and text fields, the
  Table API or the CSV export, `python` and `pyarrow` backends, `sys_id` ranges read in parallel,
  and `defer_table_reflect`

## Optimization & Configuration

Learned on the developer instance with one node (test instance), in the performance tests on two
tables of 50,000 records with 12 custom fields (performance tests), and from docs.

- **Columns**: the instance checks field ACLs for every value it returns, so the number of columns
  sets the time of a page: a page of 59 fields took about 6 times as long as one of 12 (test
  instance). Load only the columns you need with `included_columns` or `excluded_columns`.
  Benchmarks published by other connectors also show slow wide tables.
- **Partitions**: keep `allow_partitioned_load`, the default. `sys_id` ranges read in parallel were 2
  to 3 times faster than reading page by page (test instance, performance tests). `sys_id`s generated
  by the instance are random hex, so ranges split by the first hex digit hold about the same number
  of records. Chunked loads with `row_order` or `add_limit` read one page after another.
- **Extract workers** (`workers` in the `[extract]` config section): more workers help when loading
  many tables, 10 instead of 5 workers loaded 12 tables about 1.5 times faster (test instance). They do
  not speed up one table: a node runs about 4 API transactions at once and queues the rest
  ([semaphores](https://www.servicenow.com/docs/r/impact/io-semaphores-performance-metrics.html)),
  and 10 workers read one table as fast as 5. Two tables side by side reached about 1.3 times the
  throughput of one (performance tests).
- **Reader**: the Table API with `pyarrow` or `python` and the CSV export were within a few % of each
  other, the instance is the bottleneck, not the client (performance tests).
- **Page size**: on wide tables lower `page_size` so a page finishes within the transaction quota, a
  cancelled page is read again with half the records, see Quotas below. The CSV export returns at
  most 10,000 records per page.

## Code Customization

- A table of `servicenow_source`: `DEFAULT_TABLES` in `__init__.py`.
- A field type: `helpers/schema.py`, the conversion of its values in `helpers/convert.py`.
- A reader: `helpers/client.py`.
- The incremental and its query conditions: `helpers/table.py` and `helpers/query.py`.
- Parallel reads of a table: `helpers/table.py`.
- A retried response: `helpers/client.py`.
- An auth method: `helpers/credentials.py`.

## Maintenance & Troubleshooting

Facts seen on the developer instance on the Brazil release (test instance), or taken from docs and
other connectors.

- **Slow pages**: `syslog_transaction` splits the time of each request into SQL, ACL and wait times
  (test instance).
- **Sessions**: the instance runs one transaction per session. Requests that shared the session
  cookie waited 3 to 10 s each (`session_wait_time`), so the session does not keep cookies. A new
  session per request costs about 0.05 s (test instance, and
  [community article](https://www.servicenow.com/community/now-platform-articles/inbound-integrations-performance-design-on-the-servicenow/ta-p/2316967)).
- **HTTP 429**: a node runs about 4 API transactions at once and queues 50, then answers HTTP 429
  ([semaphores](https://www.servicenow.com/docs/r/impact/io-semaphores-performance-metrics.html)).
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
  - `X-Total-Count` and the aggregate API count records hidden by row ACLs, even for the admin user.
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
- **Future timestamps**: `sys_updated_on` can be in the future. The instance sets it on every
  write, but a script that turns off system fields (`autoSysFields(false)`) or an XML import writes
  any value ([community](https://www.servicenow.com/community/developer-forum/how-can-i-overwrite-the-system-fields-on-a-transform-map/m-p/1511986)).
  Such records were seen on the test instance and reported on a production instance
  ([thread](https://www.servicenow.com/community/api-insights-forum/getting-future-time-for-sys-updated-on-for-sys-user-has-role/m-p/3078906)).
  A run reads records changed before it started, so these records load only once the instance
  clock passes them, when they change again, or with `write_disposition="replace"`. Without that
  cutoff, one such record would move the cursor into the future and later runs would miss every
  change until then. When records are missing, count them with
  `sys_updated_on>javascript:gs.endOfToday()`.
- **Query literals**, checked with read-only queries (test instance):
  - Integers compare and sort as numbers, `ORDERBY` puts 9 before 10.
  - A date literal with and without `00:00:00` selects the same records.
  - Decimals, currency, longints above 2^53 and floats written without exponent compare as numbers.
    The instance does not parse exponents. Empty currencies are stored as `0`.
  - `>` works on text, but case-insensitively, while Python compares case-sensitively.
  - A condition on a field that does not exist is ignored, so all records are returned. The cursor
    must be a field in `sys_dictionary`. The dlt docs warn about invalid encoded queries.
- **Deletes**: `glide.ui.audit_deleted_tables` decides which `sys_*` tables audit deletes.
  `sys_choice` is not one of them (test instance).
- **Timestamp ties**: installs, upgrades and imports write many records within one second, e.g.
  hundreds of `sys_choice` records (test instance). When more records share one timestamp than a
  chunk holds, a chunked load with `range_start="closed"` reads the same chunk again. With
  `"open"`, it skips the rest of them.
- **Backends**: we recommend against switching backends in production, pick one while testing. By
  default, the Arrow tables of `pyarrow` get no `_dlt_load_id` and `_dlt_id`, and `_dlt_id` costs
  performance and is not needed
  ([docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas#add-_dlt_load_id-and-_dlt_id-to-your-tables)),
  but rows of `python` add both, so loads of one backend fail on a table loaded with the other
  (local). `backend="auto"` switches to `pyarrow` when it gets installed. The workaround: set
  `add_dlt_load_id` and `add_dlt_id` of `[normalize.parquet_normalizer]` for all `pyarrow` loads of
  the table.
