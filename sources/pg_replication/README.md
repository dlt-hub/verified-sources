# Postgres replication

Maintenance notes of the [Postgres](https://www.postgresql.org/) replication source for developers
and coding agents. The source reads changes of published tables from a replication slot with
[logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) and the built-in
`pgoutput` plugin. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/pg_replication).

## Changelog

### 1.0.0
**Upgrade**: install `dlt>=1.22.0` and `sqlalchemy>=2.0`. `replication_resource` continues from its
stored `last_commit_lsn` and slot, no change is missed or loaded twice. Without a reload, rows
loaded before keep the previous format until they change: `real`, `timetz`, array and domain
columns stay `text`, `timetz` and array values mix the old (`10:11:12+02`, `{1,2}`) and the new
format (`10:11:12+02:00`, `[1,2]`), and rows that a primary key update left under the old key stay.
To reload, run `dlt pipeline <name> drop <slot_name>`, which drops the replicated tables and the
state, call `init_replication` with `reset=True` and an initial snapshot, load the snapshot and run
`replication_resource`. Tables without a primary key in publications of updates or deletes stop
the run, see below. Changes are ordered by `_dlt_lsn` and deletes marked in `_dlt_deleted_ts`,
`lsn` and `deleted_ts` of tables loaded before stay empty.
- **breaking** `replication_resource`: `real` columns load as `double`, `timetz` as `time`, arrays
  as `json` and domains as their base type instead of `text`, as in snapshots
- **breaking** `init_replication` and `replication_resource`: raise `NoPrimaryKeyException` for
  tables without a primary key or replica identity index when the publication publishes updates or
  deletes. Before, Postgres rejected updates and deletes of such tables, and with
  `REPLICA IDENTITY FULL` updates were appended and deletes lost. Publish only inserts for them
- **breaking** `init_replication` and `replication_resource`: new tables and columns get no length,
  no integer size and no NOT NULL on non-key columns, so later source changes fit.
  `reflection_level="full_with_precision"` and `reflect_not_null=True` keep them. Columns loaded
  before keep their types
- **breaking** `ItemGenerator`, `get_max_lsn` and `advance_slot` are removed, imports of them fail
- **breaking** `replication_resource`: changes are ordered by `_dlt_lsn` instead of `lsn` and
  deletes marked in `_dlt_deleted_ts` instead of `deleted_ts`, so source columns with these names
  are no longer overwritten. In tables loaded before, `lsn` and `deleted_ts` stay empty in new rows
  and lose their `dedup_sort` and `hard_delete` hints
- new `init_replication`: importable from `pg_replication`, the import from `pg_replication.helpers`
  keeps working
- new `replication_resource`: a column type change whose values may not fit the destination
  column stops the run with `IncompatibleColumnTypeException`, a NULL or missing value of a NOT
  NULL destination column with `NullInNotNullColumnException`. Before, values were rounded or
  every load failed. Types in `columns` skip the check. `snapshot_pipeline` gives the types of a
  snapshot loaded by another pipeline
- new `replication_resource`: `hard_delete=False` keeps deleted rows with `_dlt_deleted_ts` set
- new `init_replication`: `initial_snapshot="table"` copies the tables, `"unlogged_table"` copies
  them without WAL, `"direct"` reads them with `sql_table` in the snapshot of the slot. Pass the
  callbacks of the returned `ReplicationSnapshot` to your own `sql_table` or `sql_database`
- new `init_replication` and `replication_resource`: `backend="pyarrow"` decodes inserts with numpy
  into Arrow tables. Switching an existing pipeline changes how `_dlt_id` is created
- new `init_replication` and `replication_resource`: `exclude_columns`, applied after
  `include_columns`
- changed `init_replication`: a publication that already publishes the tables is used as it is, so
  a user with only REPLICATION and SELECT can start the replication of tables it does not own.
  Without the privileges to create or extend the publication, `PublicationPrivilegesException`
  gives the statement for the table owner. `reset=True` keeps a publication of another role
- changed: requires `dlt>=1.22.0` and `sqlalchemy>=2.0`, direct snapshots fail on SQLAlchemy 1.4
- changed `init_replication`: snapshots are read with the session settings of the replication
  connection, role or database defaults no longer change snapshot values
- changed `init_replication`: `persist_snapshots=True` is deprecated and logs a warning, use
  `initial_snapshot="table"`
- changed `replication_resource`: an update that leaves a TOASTed value unchanged stops the run with
  `UnchangedToastException` instead of a decoding error, unless the table has
  `REPLICA IDENTITY FULL` or the column is excluded. `init_replication` warns about included
  columns at risk
- fixed `replication_resource`: a primary key update left the row under the old key
- fixed `replication_resource`: `REPLICA IDENTITY FULL` made all columns primary keys
- fixed `replication_resource`: runs reconnected for every batch (`replication slot ... is active
  for PID`) after an unbounded peek of the slot (#702)
- fixed `replication_resource`: the position was stored only when the slot was drained, runs
  bounded by `add_limit` made no progress (#703)
- fixed `replication_resource`: runs with `flush_slot=False` and no new changes hung
- fixed `replication_resource`: a schema change in the middle of a transaction loaded it twice or
  crashed the run
- fixed `replication_resource`: `Type`, `Origin` and logical messages crashed the run (#656)
- fixed `replication_resource`: failed on Postgres 10 to 12, and on the first change when
  `include_columns` or `columns` was not passed (#700)
- fixed `init_replication`: snapshots of whole-schema publications decoded their own copies (#698),
  `interval` columns failed snapshots

Versions before 1.0.0 were not numbered. Copies taken with `dlt init` have no `version.py`,
`.dlt/.sources` records the repository commit they came from.

## Extension points

- A Postgres type: its dlt column type in `to_dlt_column_type` and its converter in `_CONVERTERS`
  of `schema_types.py`, an array type in `_ARRAY_ELEMENT_TYPES`, a value Arrow cannot cast from text
  in `to_castable_array` of `arrow_helpers.py`. Add a column to `PG_TYPES_DDL` of
  `tests/pg_replication/cases.py`, `test_snapshot_types_match_replication` then compares snapshot
  and replication.
- A snapshot column type that `sql_table` reflects differently: `snapshot_type_adapter` of
  `snapshot.py`.
- A pgoutput message: its decoder in `decoders.py`, handled in `MessageConsumer.process_msg`.
- A setting that formats values sent as text: `SESSION_OPTIONS` of `schema_types.py`.

## Seeding

The credential-free tests decode pgoutput messages captured from Postgres 17:

1. Start Postgres with logical decoding, see [Tests](#tests).
2. `uv run python -m tests.pg_replication.capture_messages` from the repository root rewrites
   `tests/pg_replication/pgoutput_messages.json` with the messages of its `SCENARIOS`.
3. Update `EXPECTED_SEGMENTS` of `test_helpers.py` when a scenario changes.

## Tests

`test_pg_replication_source.py` needs Postgres with `wal_level=logical` and a user with the
`REPLICATION` attribute in `sources.pg_replication.credentials`. `tests/postgres/docker-compose.yml`
starts one. Other versions run the same way, e.g.
`docker run -p 5434:5432 -e POSTGRES_USER=loader -e POSTGRES_PASSWORD=loader -e POSTGRES_DB=dlt_data postgres:12 -c wal_level=logical`.

## Operations

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
  ([docs](https://www.postgresql.org/docs/current/sql-createpublication.html)). Openflow, Airbyte
  and Fivetran have the DBA create the publication, Debezium a role that shares the table
  ownership (#719). Creating the slot needs REPLICATION, a direct snapshot SELECT, the
  `table` and `unlogged_table` snapshots CREATE on the schema for their copies.
- **Snapshots**: the `_dlt_<table>_s_<snapshot>` copies are not dropped. The snapshot of a direct
  snapshot lives as long as the replication connection that created the slot
  ([docs](https://www.postgresql.org/docs/current/protocol-replication.html)) and holds back vacuum
  until the last table is read. Tables added to a publication later get no snapshot.
- **Performance**: 500k rows from a local Postgres 17 into `filesystem`: 173k rows/s with
  `pyarrow`, 34k with `python`, 172k receiving messages without decoding, 23k before 1.0.0. Both
  backends peak at about 240 MB (#719).
