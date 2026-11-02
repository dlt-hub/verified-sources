# Salesforce

Maintenance notes of the Salesforce source for developers and coding agents. The source reads
sObjects with [Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)
query jobs, and with the REST API for sObjects and fields the Bulk API rejects. Setup and usage are
in the [dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/salesforce).

## Changelog

### 1.0.2
**Upgrade**: safe, the incremental state carries over and the next run continues from it. Calls of
`salesforce_source` with `incremental` fail: pass `start_date`, or `write_disposition="replace"` to
reload all records on each run. `salesforce_table(incremental=None)` loads incrementally now, pass
`write_disposition="replace"` to keep reloading all records.
- new `salesforce_source` and `salesforce_table`: `start_date`, the initial value of the default
  incremental. It replaces the `incremental` argument of `salesforce_source`
- fixed all resources: incremental queries on a timestamp read records changed until the query
  starts. Records changed while a long query job runs are loaded by the next run, before they could
  move the cursor past records the job missed
- fixed `python` backend: fields longer than 131,072 characters failed. `pyarrow` backend: records
  longer than about 2 MiB failed
- fixed resources of `salesforce_source` resolve config in the `salesforce` section instead of
  `table`
- changed: requests refused because the org runs too many long requests at once are retried, a
  query job that does not complete in 24 hours is aborted and raises, a result page with fewer
  records than Salesforce counted raises, and `api="auto"` falls back to the REST API when the Bulk
  API rejects a compound field

## Extension points

- An sObject of `salesforce_source`: an entry in `SOURCE_SOBJECTS` of `__init__.py`.
- A field type: its dlt column in `sf_field_to_column_schema` of `helpers/schema.py`, types the Bulk
  API cannot select in `NON_QUERYABLE_TYPES`, cursor fields of the default incremental in
  `UPDATED_AT_FIELDS` and `CREATED_AT_FIELDS`.
- A query API: a class with the `query` method of `QueryApi` in `helpers/query.py`, yielding
  `ResultPage`s, picked in `make_query_api`. Errors after which `api="auto"` uses the REST API:
  `is_bulk_not_supported`.
- A retried error: a predicate like `is_concurrent_limit_exceeded` of `helpers/connection.py`,
  passed to dlt's `Client` in `_connection` of `__init__.py`.
- An auth method: a credentials class in `helpers/credentials.py`, added to `SalesforceAuth`.

## Seeding

The test org setup and the seeded data are described in `tests/salesforce/README.md`. Run from the
repository root, credentials are read from `sources/.dlt`:

1. `uv run python -m tests.salesforce.seed.seed deploy` creates the custom objects and fields.
2. `uv run python -m tests.salesforce.seed.seed load` writes the records of `seed/data.py`, then
   creates and deletes records so the recycle bin has deleted ones.
3. `uv run python -m tests.salesforce.seed.seed capture` rewrites `tests/salesforce/cases/`. Commit
   it, and add values Salesforce stores differently than seeded to `STORED_AS` of `seed/data.py`.

`all` runs the three steps. Each step needs a new client: the Metadata API deploy of
`simple_salesforce` leaves XML headers on the client and the following REST calls fail with 415.

## Tests

`test_salesforce_source.py` only reads the org. The deleted records it checks are purged from the
recycle bin 15 days after the last `load`, then the tests of deleted records check nothing.

## Operations

Facts seen on the Developer Edition test org with API v59.0 (test org), measured locally with
pyarrow 17 and urllib3 2.3 (local), or taken from docs and other connectors.

- **Jobs**: a Bulk API query job takes 5 to 30 s to return its first page, also for small sObjects
  (#717). `ORDER BY` is accepted (test org), but `ORDER BY` and `LIMIT` turn off the server-side
  chunking of large jobs (#717). A job without results returns a page with only the header,
  `Sforce-Locator: null` and `Sforce-NumberOfRecords: 0` (test org).
- **Limits**: the test org allows 15,000 API requests and 10,000 Bulk API 2.0 query jobs a day, and
  every response carries `Sforce-Limit-Info` (test org). Requests running longer than 20 s are
  limited to 25 at once in production and 5 in Developer Editions, refused with HTTP 403
  `REQUEST_LIMIT_EXCEEDED` and `ConcurrentPerOrgLongTxn`; the daily limit returns the same code
  ([limits](https://developer.salesforce.com/docs/platform/salesforce-app-limits-cheatsheet/guide/salesforce-app-limits-platform-api.html)).
- **REST cursors**: 30 open query cursors of one user still served their next pages, the documented
  limit of 10 was not seen (test org).
- **Truncated pages**: requests raises `ChunkedEncodingError` on a body shorter than its
  `Content-Length` or a cut chunked body, which dlt retries (local). A page with fewer records than
  `Sforce-NumberOfRecords` would come from Salesforce itself and raises.
- **CSV values**: integers come as `120`, currency and doubles of 10 million or more in exponent
  form, e.g. `9.9999999E7` (test org). Formula values are rounded to the scale of the field, also
  with division, though Fivetran
  ([docs](https://fivetran.com/docs/connectors/applications/salesforce)) reports unrounded ones
  (test org). `SystemModstamp` has second precision (test org). Pages have no byte order mark (test
  org).
- **Long values**: long text areas hold up to 131,072 characters (test org), the default field limit
  of Python's `csv`. An Apex class holds up to 1 million
  ([limits](https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm)).
  pyarrow fails on a record longer
  than two parse blocks and then aborts the process when it exits. Blocks of 8 MiB parse records up
  to about 16 MiB: on a 40 MiB page they take 13 ms longer than blocks of 1 MiB and Arrow allocates
  about 190 MiB instead of 60 MiB, with the same RSS (local). Parsing a page in one block allocated
  about 770 MiB.
- **Deletes**: `queryAll` returns deleted `OpportunityLineItem` records, deleted on their own or
  with their opportunity, with `IsDeleted=true` (test org), though Fivetran
  ([docs](https://fivetran.com/docs/connectors/applications/salesforce)) reports they are not.
  Deleted records stay in the recycle bin for 15 days, records purged between two runs stay in the
  destination.
- **Changes without `SystemModstamp`**: formula fields that read related records or use `TODAY()`,
  and sharing changes such as account and opportunity team members, change without changing
  `SystemModstamp` ([Fivetran docs](https://fivetran.com/docs/connectors/applications/salesforce)).
  Incremental loads keep the old values until the sObject is reloaded.
- **Compound fields**: the Bulk API rejects some compound fields with `Selecting compound data not
  supported in Bulk Query`, Airbyte and Estuary read such sObjects with the REST API.
- **Sessions**: a long job may outlive the session. The source logs in again and continues with the
  same job and page, which does not work for credentials with a session id only (#717).
- **Throughput**: 50,000 rows with 36 fields load to the local filesystem in 0.15 s with `pyarrow`
  and 2.1 s with `python` (#717).
