# MongoDB

Maintenance notes of the [MongoDB](https://www.mongodb.com/) source. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb).

## Changelog

### 1.0.0
**Upgrade**: safe, install `dlt>=1.16.0` and `pymongo[zstd]>=4.4`. The incremental state carries over
and the `python` backend loads the same tables. `data_item_format` still works with a deprecation
warning. With the `pyarrow` backend nested documents already loaded as `json` columns stay `json`,
new nested fields are flattened into columns.
- fixed `mongodb_collection`: failed with `InvalidResourceReturnsResource` on dlt 1.12 and later
- fixed `pyarrow` backend: failed to import with `pymongoarrow>=1.6`
- fixed `pyarrow` backend: the incremental cursor is filtered on the server, before the whole
  collection was read
- fixed: a projection with an incremental keeps the cursor field, also when the projection
  includes the document that holds it. Excluding that document raises
- fixed: a filter on the cursor field no longer replaces the incremental filter
- fixed `python` backend: DBRef, MinKey, MaxKey, PCRE-only regexes and dates outside the Python
  range failed the load. Such dates are clamped
- fixed `pyarrow` backend: dlt warned about the nullability of `_id` for every table
- changed `parallel`: reads `_id` partitions of about `partition_size_bytes` instead of skip and
  limit batches, which can return a document twice or miss it. With `limit` it reads in one query
- changed `parallel` with the `pyarrow` backend: partitions are parsed in worker processes, at most
  `workers` of the `extract` config for all collections, and each loads as one table
- changed `pyarrow` backend: nested documents are flattened up to `max_table_nesting`, except
  `json` columns
- changed `pyarrow` backend: timestamps are UTC, ObjectIds in nested documents and lists load as
  strings
- changed `pyarrow` backend: when a field changes type between batches, numbers are cast to its
  first type and other values load as null, before the load failed
- changed `pyarrow` backend: values that do not match `pymongoarrow_schema` raise
  `FieldTypeMismatchError`
- changed: the `python` backend yields `datetime` in UTC instead of `pendulum.DateTime`
- changed `mongodb`: `system.*` collections are no longer loaded unless named in `collection_names`
- changed `mongodb_collection`: `data_item_format` is deprecated, use `backend` with `python`
  (default) or `pyarrow`
- new: the server compresses the documents it sends with zstd, snappy or zlib, unless the
  connection URL sets `compressors`
- new `pyarrow` backend: `on_inferred_type_mismatch`
- new: `partition_size_bytes` of `parallel` reads, 16 MiB by default
- new `mongodb`: `backend` and `chunk_size` arguments

## Tests

- `test_local_*` need the local MongoDB: `docker compose -f tests/mongodb/docker-compose.yml up -d`.
  It runs MongoDB 7 because 8.0 and later crash at start on Linux 6.19 and newer, like GitHub runners
  ([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)).
- Incremental and ordering tests run only on the local MongoDB. The other tests in
  `test_mongodb_source.py` read samples of the `sample_mflix` dataset from the `connection_url` in
  `sources/.dlt/secrets.toml`, `test_all_resources` loads all of it.
- Run the `pyarrow` backend tests with the oldest and newest `pymongoarrow`:
  `uv run --with pymongoarrow==1.16.1 pytest tests/mongodb`.

## Operations

- `pymongoarrow` pins one `pyarrow` minor version per release, so the installed `pyarrow` decides
  its version ([changelog](https://github.com/mongodb-labs/mongo-arrow/blob/main/bindings/python/CHANGELOG.md)).
  Before 1.6 nested ObjectIds load as binary. 1.6.0 to 1.6.3 fail when no document of a batch has
  a nested field ([#695](https://github.com/dlt-hub/verified-sources/issues/695)). Before 1.9
  values that do not match `pymongoarrow_schema` load as null. Before 1.13 an int64 after int32
  values fails with `OverflowError`.
- `pymongoarrow` infers the type of a field from its first value in a batch. It loads regex, BSON
  timestamp, code with scope, symbol, MinKey and MaxKey as null. Such collections need
  `pymongoarrow_schema` or the `python` backend. Arrays load as child tables with `python` and as
  `json` with `pyarrow`, so a loaded resource needs a reload to switch the backend.
- The primary key is `_id`. Views and time series collections can return documents without `_id`
  and the load fails. Set another key, or none with `apply_hints(primary_key=())`.
- `parallel` sizes partitions from the average document size in `$collStats`. Partitions shrink
  down to 1 MiB until each extract worker gets 8. The planner reads `workers` from the `extract`
  config and does not see the `workers` argument of `pipeline.extract`.
- Without a filter, partitions are `min`/`max` bounds on the `_id` index. With a filter (an
  incremental with a last value or `filter_`), partitions are `$gte`/`$lt` ranges of the most common
  `_id` type and one query for each other type
  ([type bracketing](https://www.mongodb.com/docs/manual/reference/mql/query-predicates/type-bracketing/)).
  Planning scans the matching documents, index the cursor field. Views, time series collections
  and collections whose planning query fails are read in one query.
- Parallel `pyarrow` reads start one worker process per extract thread with `sys.executable`. Where
  it is not a Python interpreter (uwsgi), use `parallel=False`. A worker holds about three copies
  of its partition, dlt keeps up to `max_parallel_items` (20) partitions in flight.
- Loading in chunks with `add_limit` and `row_order` needs `parallel=False`. dlt evaluates parallel
  reads ahead and its incremental moves past the reads that `add_limit` drops.
- 1M local documents read in 11 s with `pyarrow` and 4.4 s with 8 workers. `python` takes 30 s
  with or without `parallel` because decoding holds the GIL. Compression makes Atlas reads 2.5x
  faster and local reads 30% slower, disable it with `compressors=` in the connection URL.
