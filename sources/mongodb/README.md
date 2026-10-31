# MongoDB

Maintenance notes of the [MongoDB](https://www.mongodb.com/) source. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb).

## Changelog

### 1.0.0
**Upgrade**: safe, install `dlt>=1.16.0` and `pymongo[zstd]>=4.4`. The incremental state carries over
and the object format loads the same tables. In the Arrow format nested documents already loaded as
`json` columns stay `json`, new nested fields are flattened into columns.
- fixed `mongodb_collection`: failed with `InvalidResourceReturnsResource` on dlt 1.12 and later
- fixed Arrow: failed to import with `pymongoarrow>=1.6`
- fixed Arrow: the incremental cursor is filtered on the server, before the whole collection was read
- fixed: a projection with an incremental keeps the cursor field, also when the projection
  includes the document that holds it. Excluding that document raises
- fixed: a filter on the cursor field no longer replaces the incremental filter
- fixed object format: DBRef, MinKey, MaxKey, PCRE-only regexes and dates outside the Python range
  failed the load. Such dates are clamped
- fixed Arrow: dlt warned about the nullability of `_id` for every table
- changed `parallel`: reads `_id` partitions of about `partition_size_bytes` instead of skip and
  limit batches, which could return a document twice or miss it. Without a filter the partitions
  are bounds on the `_id` index, with one they are planned on the matching documents as ranges
  of one `_id` type, which the Arrow format also uses for mixed `_id` types. With `limit` it
  reads in one query
- changed `parallel` Arrow: partitions are parsed in worker processes, at most `workers` of the
  `extract` config for all collections, and each loads as one table
- changed Arrow: nested documents are flattened up to `max_table_nesting`, except `json` columns
- changed Arrow: timestamps are UTC, ObjectIds in nested documents and lists load as strings
- changed Arrow: when a field changes type between batches, numbers are cast to its first type and
  other values load as null, before the load failed
- changed Arrow: values that do not match `pymongoarrow_schema` raise `FieldTypeMismatchError`
- changed: the object format yields `datetime` in UTC instead of `pendulum.DateTime`
- changed `mongodb`: `system.*` collections are no longer loaded unless named in `collection_names`
- new: the server compresses the documents it sends with zstd, snappy or zlib, unless the
  connection URL sets `compressors`
- new Arrow: `on_inferred_type_mismatch`
- new: `partition_size_bytes` of `parallel` reads, 16 MiB by default, smaller when the extract
  workers would get fewer than 8 partitions each
- new `mongodb`: `data_item_format` and `chunk_size` arguments

## Tests

- `test_local_*` need the local MongoDB: `docker compose -f tests/mongodb/docker-compose.yml up -d`.
  It runs MongoDB 7, 8.0 and later crash at start on Linux 6.19 and newer, e.g. GitHub runners
  ([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)).
- Incremental and ordering tests run only on the local MongoDB. The other tests in
  `test_mongodb_source.py` read samples of the `sample_mflix` dataset from the `connection_url` in
  `sources/.dlt/secrets.toml`, `test_all_resources` loads all of it.
- Run the Arrow tests with the oldest and newest `pymongoarrow`, e.g.
  `uv run --with pymongoarrow==1.16.1 pytest tests/mongodb`, the dev lock may pin older ones.

## Operations

- `pymongoarrow` pins one `pyarrow` minor version per release (1.16 needs `pyarrow` 26), so the
  installed `pyarrow` decides which `pymongoarrow` users get
  ([changelog](https://github.com/mongodb-labs/mongo-arrow/blob/main/bindings/python/CHANGELOG.md)):
  - 1.6 changed the API of `PyMongoArrowContext` and stopped resetting it on `finish()`;
  - 1.6.0 to 1.6.3 fail on a batch where every document misses a struct field
    ([#695](https://github.com/dlt-hub/verified-sources/issues/695));
  - before 1.6 ObjectIds in nested documents and lists load as binary;
  - before 1.9 values that do not match `pymongoarrow_schema` load as null;
  - before 1.13 an int64 after int32 values in a batch fails with `OverflowError`.
- `pymongoarrow` infers the type of a field from its first value in a batch and loads values of
  other types as null or truncated (`2.5` as `2`). It does not convert regex, BSON timestamp, code
  with scope, symbol, MinKey and MaxKey, they load as null. Collections with such fields need
  `pymongoarrow_schema` or the object format. Declare ObjectIds as `ObjectIdType()` and code as
  `CodeType()` in the schema, fields not in it are not loaded.
- Arrays load as child tables in the object format and as `json` in the Arrow format. Switching the
  format of a loaded resource needs a reload, e.g. `refresh="drop_resources"`.
- The primary key of the resources is `_id`. A null `_id` is valid in MongoDB, dlt rejects it.
  Views and time series collections may return documents without `_id`, e.g. a view with
  `$project: {_id: 0}` or measurements inserted without it, the server does not add it (this PR,
  local MongoDB). The load then fails, set another key with `apply_hints(primary_key=...)` or none
  with `()`. Views may also repeat `_id`, e.g. with `$unwind`, which collapses rows with `merge`.
- `parallel` sizes its partitions from `$collStats`: `partition_size_bytes` divided by the average
  document size is the number of documents in a partition, `chunk_size` when the server has no
  stats. Partitions shrink, down to 1 MiB, until there are 4 per extract worker, so that the last
  round of reads leaves few workers idle (this PR: 11 partitions of 16 MiB for 8 workers read a
  170 MiB Atlas collection 20-30% slower than 32 of 5 MiB). The planner takes `workers` from the
  `extract` config, e.g. `EXTRACT__WORKERS`, a `workers` argument of `pipeline.extract` is not
  visible to it. Partitions are planned from about 10 sampled `_id`s each, so they hold 0.3 to 2
  times their planned size (this PR, 1M local documents).
- A read without a filter splits the `_id` index into `min`/`max` bounds with a hint on it, from
  a `$sample` and `$bucketAuto` over all `_id`s. Index bounds are not type bracketed, so every
  BSON type of `_id` lands in one partition, and the server compares them with the default
  collation of the collection, which the source never overrides. A MaxKey `_id` ends the index
  and is read with its own query. The Arrow format uses the bounds only when `_id` holds one
  BSON type: pymongoarrow infers one type per column, and a batch with e.g. date and timestamp
  `_id`s fails while other mixes load as null.
- A read with a filter (an incremental with a last value, `filter_`) counts the matching
  documents with `count_documents` and samples them behind `$match`, so a selective incremental
  reads in one query. Its partitions are `$gte`/`$lt` ranges of the most common `_id` type, which
  match only that type
  ([type bracketing](https://www.mongodb.com/docs/manual/reference/mql/query-predicates/type-bracketing/)),
  plus one query per other `_id` type, which reads all its documents at once. Index bounds would
  need the hint on `_id` and keep the filter from using its own index, so filtered reads do not
  use them. Index the cursor field: the count and the sample scan the matching documents before
  the read.
- Views and time series collections have no `_id` index and are read in one query, clustered
  collections have no separate one and use the ranges. When the server rejects a planning query,
  e.g. `$where` in `filter_` or a server without `$bucketAuto`, the collection is read in one
  query with a warning.
- `$sample` of more than 5% of a collection scans it, and so does `$sample` behind `$match`
  ([docs](https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/)).
- dlt keeps up to `max_parallel_items` partitions (20 by default) in flight, and an Arrow worker
  holds about three copies of the partition it parses: the raw BSON, the table and its IPC buffer.
- Read times on a local MongoDB, 1M documents of about 700 B, no compression (this PR):
  - Arrow: 11 s, `parallel` with 4 workers 4.5 s, with 8 workers 3.5 s;
  - object format: 29.5 s, with 4 threads 33 s, decoding holds the GIL.
- Reads from Atlas are bandwidth bound, 300k documents (170 MiB) take (this PR):
  - uncompressed: 78 s, object format with 8 workers 47 s;
  - zstd: 30 s, object format with 8 workers 14 s, Arrow 20 s, Arrow with 8 workers 11 s;
  - zlib and snappy were about as fast as zstd.
- On a server on the same machine or network compression makes reads about 30% slower, set
  `compressors=` in the connection URL.
- `pymongo[zstd]` installs `backports.zstd` before Python 3.14 and `zstandard` for pymongo before
  4.16.
- Parallel Arrow reads start a worker process per extract thread with `sys.executable`, in about
  0.4 s. Where it is not a Python interpreter (e.g. uwsgi) use `parallel=False`.
- Loading in chunks with `add_limit` and `row_order` needs `parallel=False`. dlt evaluates parallel
  reads ahead and its incremental moves the last value past the reads that `add_limit` drops.
