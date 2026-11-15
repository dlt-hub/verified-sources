# Postgres replication

Maintenance notes of the [Postgres](https://www.postgresql.org/) replication source for developers
and coding agents. The source reads changes of published tables from a replication slot with
[logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) and the built-in
`pgoutput` plugin. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/pg_replication).

## Changelog

### 1.0.0
**Upgrade**: install `dlt>=1.22.0` and `sqlalchemy>=2.0`. `replication_resource` continues from its
stored position, no change is missed or loaded twice. Without a reload, rows loaded before keep the
old formats until they change: `real`, `timetz`, array and domain columns stay `text` and mix old
and new values, and rows that a primary key update left under the old key stay. To reload, run
`dlt pipeline <name> drop <slot_name>`, call `init_replication` with `reset=True` and an initial
snapshot, load the snapshot and run `replication_resource`. Tables without a primary key in
publications of updates or deletes stop the run.
- **breaking** `replication_resource`: `real` loads as `double`, `timetz` as `time`, arrays as
  `json` and domains as their base type instead of `text`
- **breaking** `init_replication` and `replication_resource`: tables without a primary key or
  replica identity index raise `NoPrimaryKeyException` when updates or deletes are published
- **breaking** `init_replication` and `replication_resource`: new columns get no length, integer
  size and NOT NULL, `reflection_level="full_with_precision"` and `reflect_not_null=True` keep them
- **breaking** `replication_resource`: changes are ordered by `_dlt_lsn` and deletes marked in
  `_dlt_deleted_ts` instead of `lsn` and `deleted_ts`, which stay empty in tables loaded before
- **breaking** `ItemGenerator`, `get_max_lsn` and `advance_slot` are removed
- new `init_replication`: `initial_snapshot` loads the tables as of the slot, also tables added to
  an existing slot
- new `replication_slot_exists`
- new `init_replication` and `replication_resource`: `backend="pyarrow"` and `exclude_columns`
- new `replication_resource`: `hard_delete=False` keeps deleted rows
- changed `replication_resource`: values that may not fit the destination column and unchanged
  TOASTed values stop the run, before values were rounded or loads failed
- changed `init_replication`: a publication that already publishes the tables is used as it is,
  `persist_snapshots=True` is deprecated
- changed `init_replication` and `replication_resource`: tables or columns whose names normalize to
  the same name raise, before they loaded into one table or column
- fixed `replication_resource`: a primary key update left the row under the old key,
  `REPLICA IDENTITY FULL` made all columns primary keys, a schema change within a transaction
  loaded it twice
- fixed `replication_resource`: reconnects for every batch (#702), no progress with `add_limit`
  (#703), hangs with `flush_slot=False`, crashes on Postgres 10 to 12 (#700) and on `Type`,
  `Origin` and logical messages (#656)
- fixed `init_replication`: snapshots of whole-schema publications (#698), of `interval` and
  generated columns, and with role or database session defaults
- fixed: `dlt init` failed without `psycopg2`

Versions before 1.0.0 were not numbered. Copies taken with `dlt init` have no `version.py`,
`.dlt/.sources` records the repository commit they came from.

## Optimization & Configuration

Measured with 500,000 rows from a local Postgres 17 (#719).

- **Backend**: `pyarrow` is about 5 times faster than `python`, with about the same peak memory.
  With `pyarrow` decoding is not the bottleneck: receiving the messages without decoding them was as
  fast. Pick one before the initial load, see Backends below.

## Code Customization

- A Postgres type: `schema_types.py`, its Arrow cast in `arrow_helpers.py`, and a column in the test
  cases of `tests/pg_replication/cases.py`.
- A snapshot column type that `sql_table` reflects differently: `snapshot.py`.
- A pgoutput message: `decoders.py`.
- A setting that formats values sent as text: `SESSION_OPTIONS` in `schema_types.py`.

## Maintenance & Troubleshooting

- **Versions**: Postgres 10, 12, 14 and 17 pass the tests (#719). Whole-schema publications need
  Postgres 15 ([docs](https://www.postgresql.org/docs/15/sql-createpublication.html)), logical
  decoding on a standby Postgres 16
  ([docs](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html)).
- **WAL**: a slot keeps WAL until its consumer confirms the changes
  ([docs](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS)).
  Changes are confirmed at the start of the next run, so a pipeline that stops running fills the
  disk of the server. With `flush_slot=False`, psycopg2 still confirms keepalive positions in runs
  without changes (Postgres 17, #719).
- **Session settings**: pgoutput formats values as text with the settings of the session. Role or
  database defaults such as `datestyle=SQL,DMY`, `intervalstyle=iso_8601` or `extra_float_digits=0`
  changed snapshot values until `SESSION_OPTIONS` pinned them for snapshots too (Postgres 17, #719).
- **Numeric**: `numeric` without precision loads as dlt's `decimal(38, 9)`. With more decimals the
  `pyarrow` backend fails with `Conversion to arrow failed for field ...` and the `python` backend
  loads values cut to 9 decimals (DuckDB, #719). Setting precision and scale in `columns` of
  `init_replication` and `replication_resource` fixes both. `NaN` loads as NULL with a warning on
  `pyarrow`, `Infinity` and `-Infinity` (Postgres 14+,
  [docs](https://www.postgresql.org/docs/14/datatype-numeric.html)) fail its cast, the `python`
  backend passes all three to the destination (#719).
- **TOAST**: an update sends a value stored out of line only when it changes or the table has
  `REPLICA IDENTITY FULL`
  ([docs](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)).
  Reading it again from the table would see a newer row. Any `text`, `json`, array or `bytea`
  column may be stored out of line, `init_replication` names the included ones.
- **Replica identity**: Postgres rejects updates and deletes of a table without a replica identity
  in a publication of updates or deletes
  ([docs](https://www.postgresql.org/docs/current/logical-replication-publication.html)).
  With `REPLICA IDENTITY USING INDEX` deletes carry only the index columns, so replication merges on
  them while snapshots merge on the primary key. Both identify the same rows (#719).
- **Type changes**: Postgres sends the new column type in the next Relation message
  ([docs](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)), dlt
  never alters the type of a destination column. Without the type check int→numeric(12,2) and
  numeric(7,2)→numeric(6,3) loaded rounded values, varchar(5)→varchar(50) and int→text failed
  every load (Snowflake and DuckDB, #719).
- **Privileges**: creating a publication needs CREATE on the database, adding a table to it the
  ownership of the table, `FOR TABLES IN SCHEMA` a superuser
  ([docs](https://www.postgresql.org/docs/current/sql-createpublication.html)). Other connectors
  have the DBA create the publication, or use a role that shares the table ownership (#719). Creating the slot needs REPLICATION, a direct snapshot SELECT, the
  `table` and `unlogged_table` snapshots CREATE on the schema for their copies. Row-level
  security filters snapshots of a role without BYPASSRLS but not replicated changes, so
  `SESSION_OPTIONS` sets `row_security=off` and such snapshots fail
  ([docs](https://www.postgresql.org/docs/current/logical-replication-security.html)).
- **Skipping changes**: `SELECT pg_replication_slot_advance('<slot>', '<LSN>')` (Postgres 11+,
  [docs](https://www.postgresql.org/docs/current/functions-admin.html)) while no run is
  active makes the slot skip transactions committed before the LSN, the next run logs that the
  slot is past the stored position and continues. Errors caused by a change give the LSN after its
  transaction (Postgres 12 and 17). It also skips all changes of the published tables not loaded yet: a
  slot can't skip a single transaction, Postgres' own subscriber drops one while applying changes
  (`ALTER SUBSCRIPTION ... SKIP`,
  [docs](https://www.postgresql.org/docs/current/logical-replication-conflicts.html)). Errors of
  changed table columns can't be skipped, later transactions have the same columns.
- **Not replicated**: `TRUNCATE` (ignored with a warning); defaults of added columns, existing rows
  get them without WAL; generated columns before Postgres 18; a renamed table loads into a new
  table, a dropped and re-created one leaves a publication of listed tables. Partitions replicate
  under their own names as the publication is created without `publish_via_partition_root`.
  Snapshots ignore row filters and column lists of the publication (Postgres 15+)
  ([docs](https://www.postgresql.org/docs/current/logical-replication-restrictions.html)).
- **Slots**: `max_slot_wal_keep_size` (Postgres 13+) and `idle_replication_slot_timeout`
  (Postgres 18) invalidate a slot, also one idle between scheduled runs
  ([docs](https://www.postgresql.org/docs/current/runtime-config-replication.html)). Creating a
  slot waits for running transactions, `init_replication` hangs while a session is idle in a
  transaction.
- **Snapshots**: the `_dlt_<table>_s_<snapshot>` copies are not dropped. The snapshot of a direct
  snapshot lives as long as the replication connection that created the slot
  ([docs](https://www.postgresql.org/docs/current/protocol-replication.html)) and holds back vacuum
  until the last table is read. On an existing slot the tables are read as they are and the
  replayed changes are merged over them (Postgres 10 and 17).
- **Backends**: we recommend against switching backends in production, pick one while testing and
  use it for the initial load too. By default, the Arrow tables of `pyarrow` get no `_dlt_load_id`
  and `_dlt_id`, and `_dlt_id` costs performance and is not needed
  ([docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas#add-_dlt_load_id-and-_dlt_id-to-your-tables)),
  but rows of `python` add both, so loads of one backend fail on a table loaded with the other
  (local). The workaround: set `add_dlt_load_id` and `add_dlt_id` of
  `[normalize.parquet_normalizer]` for all `pyarrow` loads of the table.
