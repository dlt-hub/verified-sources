# MongoDB

Maintenance notes of the [MongoDB](https://www.mongodb.com/) source for developers and coding
agents. The source reads collections with `pymongo`, as Python objects or, with `pymongoarrow`
installed, as Arrow tables. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb).

## Changelog

### 1.0.0
**Upgrade**: safe, install `dlt>=1.16.0` and `pymongo>=4.4`. The incremental state carries over
and the object format loads the same tables. In the Arrow format nested documents already loaded as
`json` columns stay `json`, new nested fields are flattened into columns.
- fixed `mongodb_collection`: iterating or running the resource failed with
  `InvalidResourceReturnsResource` on dlt 1.12 and later
- fixed Arrow (`data_item_format="arrow"`): failed to import with `pymongoarrow>=1.6`
- fixed Arrow: the incremental cursor is filtered on the server, before every document of the
  collection was read
- fixed `parallel`: batches are sorted by `_id`, so skip and limit no longer return a document
  twice or skip it, and the parallel Arrow loader runs in the thread pool
- fixed: a projection with an incremental keeps the cursor field. Before the run failed with
  `IncrementalCursorPathMissing`
- fixed: a filter on the cursor field (e.g. `{"$exists": True}`) no longer replaces the
  incremental filter on the server
- changed Arrow: nested documents are flattened into columns up to `max_table_nesting` of the
  resource or source, like in the object format (`address__city`), except columns typed `json`.
  Arrays and deeper documents load as `json`, as native nested types on destinations that support
  them (e.g. `filesystem` with parquet)
- changed Arrow: values that do not match `pymongoarrow_schema` raise `FieldTypeMismatchError`
  with `pymongoarrow>=1.9`. Older versions still load them as null
- new Arrow: `on_inferred_type_mismatch` for collections loaded without `pymongoarrow_schema`,
  where values whose type differs from the first type of their field load as null: "ignore"
  (default) as before, "warn" and "raise" detect them with a second, strict parse of each batch
  (`pymongoarrow>=1.9`). When the type of a field changes between batches its values load as null,
  before the load failed
- changed Arrow: timestamps are UTC (`timestamp[ms, tz=UTC]`), destination columns stay the same
- changed Arrow: ObjectIds in nested documents and lists load as strings like in the object
  format, with `pymongoarrow>=1.6`
- changed: the object format yields `datetime` in UTC instead of `pendulum.DateTime`
- fixed object format: DBRef (loads as `$ref`, `$id`, `$db`), MinKey and MaxKey (load as null)
  and regexes with PCRE only syntax failed the load
- fixed object format: dates before year 1 or after 9999 are clamped to the first or last
  millisecond of the Python range, before they failed the load
- changed `mongodb`: `system.*` collections (e.g. `system.views`, the raw buckets of time series
  collections) are no longer loaded unless named in `collection_names`. Tables loaded from them
  before stay in the destination
- new `mongodb`: `data_item_format` and `chunk_size` arguments

## Tests

- Unit tests mock the collection with real BSON documents and run the real `pymongoarrow`.
- `test_local_*` tests need the local MongoDB:
  `docker compose -f tests/mongodb/docker-compose.yml up -d`. They seed and drop their own
  database.
- Other tests in `test_mongodb_source.py` read the `sample_mflix` sample dataset from the
  `connection_url` in `sources/.dlt/secrets.toml`.
- Run the Arrow tests with the oldest and newest `pymongoarrow`, e.g.
  `uv run --with pymongoarrow==1.16.1 pytest tests/mongodb`, the dev lock may pin older ones.

## Operations

- `pymongoarrow` pins one `pyarrow` minor version per release (1.16 needs `pyarrow` 26), so the
  installed `pyarrow` decides which `pymongoarrow` users get. The source supports 1.5 and later:
  1.6 replaced `PyMongoArrowContext.from_schema` and `pymongoarrow.lib.process_bson_stream` and
  stopped resetting the context on `finish()`, so each batch gets a new context.
  ([changelog](https://github.com/mongodb-labs/mongo-arrow/blob/main/bindings/python/CHANGELOG.md))
- `pymongoarrow` 1.6.0 to 1.6.3 fail on a batch where every document misses a struct field
  ([#695](https://github.com/dlt-hub/verified-sources/issues/695)).
- `pymongoarrow` before 1.6 loses the ObjectId type in nested documents and lists, they load as
  binary.
- `pymongoarrow_schema` is enforced with the strict parsing of `pymongoarrow` 1.9 (changelog),
  older versions load values that do not match it as null. Declare ObjectIds as `ObjectIdType()`
  and code as `CodeType()`, not `pa.string()`. Fields not in the schema are not loaded.
- Both formats flatten nested documents into columns (`address__city`) up to `max_table_nesting`.
  Arrays become child tables in the object format and `json` columns in the Arrow format, dlt does
  not unnest Arrow lists. Switching the format of a loaded resource needs a reload, e.g.
  `refresh="drop_resources"`.
- `pymongoarrow` infers the type of a field from its first value in each batch and loads values of
  other types as null or truncated (`2.5` as `2`). `ArrowBatchConverter` parses each batch again
  with the inferred schema, strict since `pymongoarrow` 1.9, to detect it, and compares field types
  between batches; integers of any size count as one type. Before `pymongoarrow` 1.13 an int64
  after int32 values in a batch fails with `OverflowError`.
- `pymongoarrow` does not convert regex, BSON timestamp, code with scope, symbol, MinKey and
  MaxKey, they load as null in the Arrow format.
- Decimal128 loads as text in both formats. Arrow converts it value by value, `pa.decimal128` in
  `pymongoarrow_schema` (`pymongoarrow>=1.7`) loads it as decimal instead.
- `parallel` reads batches with skip and limit, the server scans all skipped documents
  ([docs](https://www.mongodb.com/docs/manual/reference/method/cursor.skip/)).
