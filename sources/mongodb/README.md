# MongoDB

Maintenance notes of the [MongoDB](https://www.mongodb.com/) source. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb).

## Changelog

### 1.0.0
**Upgrade**: safe, install `dlt>=1.16.0` and `pymongo[zstd]>=4.4`. The incremental state carries
over. With the `pyarrow` backend, nested documents loaded as `json` columns stay `json`, new nested
fields load as columns.
- fixed `mongodb_collection`: failed on dlt 1.12 and later
- fixed `pyarrow` backend: failed with `pymongoarrow>=1.6`, and read the whole collection with an
  incremental
- fixed: a projection dropped the cursor field, a filter on the cursor field replaced the
  incremental
- fixed `python` backend: DBRef, MinKey, MaxKey, PCRE-only regexes and dates outside the Python
  range failed the load. Such dates are clamped
- fixed `parallel`: documents were loaded twice or missed
- changed `pyarrow` backend: nested documents load as columns up to `max_table_nesting`, timestamps
  in UTC, nested ObjectIds as strings, values of a field that changes type are cast or null instead
  of failing the load
- changed `pyarrow` backend: values that do not match `pymongoarrow_schema` raise
  `FieldTypeMismatchError`
- changed `python` backend: `datetime` in UTC instead of `pendulum.DateTime`
- changed `mongodb`: `system.*` collections load only when named in `collection_names`
- changed `mongodb_collection`: `data_item_format` is deprecated, use `backend`
- new `mongodb`: `backend` and `chunk_size`
- new `pyarrow` backend: `on_inferred_type_mismatch`, `parallel` reads in worker processes and
  `partition_size_bytes`
- new: network compression with zstd, snappy or zlib

## Maintenance & Troubleshooting

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
- We recommend against switching backends in production, pick one while testing. By default, the
  Arrow tables of `pyarrow` get no `_dlt_load_id` and `_dlt_id`, and `_dlt_id` costs performance and
  is not needed
  ([docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas#add-_dlt_load_id-and-_dlt_id-to-your-tables)),
  but rows of `python` add both, so loads of one backend fail on a table loaded with the other
  (local). `pyarrow` falls back to `python` when `pymongoarrow` is not installed. The workaround for
  collections without arrays: set `add_dlt_load_id` and `add_dlt_id` of
  `[normalize.parquet_normalizer]` for all `pyarrow` loads of the table.
- The primary key is `_id`. Views and time series collections can return documents without `_id`
  and the load fails. Set another key, or none with `apply_hints(primary_key=())`.
- Deleted documents stay in the destination: the source reads with `find`, which does not return
  them (`test_local_incremental_changes`). Reload the collection, or soft delete documents and set
  their cursor field. An update loads only when it also changes the cursor field.
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
