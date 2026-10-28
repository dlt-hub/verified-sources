# Postgres replication

Maintenance notes of the [Postgres](https://www.postgresql.org/) replication source for developers
and coding agents. The source reads changes of published tables from a replication slot with
[logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) and the built-in
`pgoutput` plugin. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/pg_replication).

## Changelog

### 1.0.0
**Upgrade**: install `dlt>=1.22.0` and `sqlalchemy>=2.0`. Existing pipelines then continue from
the stored `last_commit_lsn` and their slot, no changes are missed or loaded again, except for
tables without a primary key, which now raise `NoPrimaryKeyException` (see below). Rows loaded
before stay as the previous version wrote them until they change: `real`, `timetz`, array and
domain columns keep the `text` type, with `timetz` and array values in the old format
(`10:11:12+02`, `{1,2}`) next to the new one (`10:11:12+02:00`, `[1,2]`), and rows that primary
key updates left under the old key stay. To reload with the new types, run
`dlt pipeline <name> drop <slot_name>`, which drops the replicated tables and the state, then
`init_replication` with `reset=True` and an initial snapshot, load the snapshot and run
`replication_resource`.
- **breaking** `replication_resource`: `real` columns load as `double`, `timetz` as `time`, arrays
  as `json` and domains as their base type instead of `text`, the same as in snapshots
- **breaking** `ItemGenerator`, `get_max_lsn` and `advance_slot` are removed, `MessageConsumer`
  yields segments of changes
- **breaking** `init_replication` and `replication_resource` raise `NoPrimaryKeyException` for
  tables without a primary key or replica identity index when updates or deletes are published.
  Postgres rejected their updates and deletes, with `REPLICA IDENTITY FULL` updates were appended
  and deletes lost. Publish only inserts for such tables
- new `initial_snapshot` of `init_replication`: `"table"` copies the tables, `"unlogged_table"`
  copies them without WAL, `"direct"` reads them with `sql_table` in the snapshot of the slot. Pass
  the callbacks of the returned `ReplicationSnapshot` to your own `sql_table` or `sql_database`
- new `backend="pyarrow"` of `replication_resource` and `init_replication`: inserts are decoded
  with numpy into Arrow tables. Switching an existing pipeline changes how `_dlt_id` is created
- new `exclude_columns` of `replication_resource` and `init_replication`, applied after
  `include_columns`
- changed: requires `dlt>=1.22.0` and `sqlalchemy>=2.0`. Direct snapshots fail on SQLAlchemy 1.4,
  which reads the live tables instead of the snapshot
- changed: snapshots are read with the session settings of the replication connection, so role or
  database defaults (`datestyle`, `intervalstyle`, `extra_float_digits`) do not change values
- changed: `persist_snapshots=True` is deprecated and logs a warning, use
  `initial_snapshot="table"`
- changed: an update that leaves a TOASTed value unchanged takes it from the old row with
  `REPLICA IDENTITY FULL`, otherwise it stops the run with `UnchangedToastException` instead of a
  decoding error, unless the column is excluded. `init_replication` warns about included columns
  at risk
- fixed: primary key updates left the row under the old key
- fixed: runs reconnected for every batch (`replication slot ... is active for PID`) after an
  unbounded peek of the slot (#702)
- fixed: the position was stored only when the slot was drained, runs bounded by `add_limit` made
  no progress (#703)
- fixed: runs with `flush_slot=False` and no new changes hung
- fixed: a schema change in the middle of a transaction loaded it twice or crashed the run
- fixed: `Type`, `Origin` and logical messages crashed the run (#656)
- fixed: snapshots of whole-schema publications decoded their own copies (#698), `interval`
  columns failed snapshots, `REPLICA IDENTITY FULL` made all columns primary keys
- fixed: `replication_resource` failed on Postgres 10 to 12, `(None,)` defaults (#700)

Versions before 1.0.0 were not numbered. Copies taken with `dlt init` have no `version.py`,
`.dlt/.sources` records the repository commit they came from.

## Extension points

- A Postgres type: its dlt column type in `to_dlt_column_type` and its converter in `_CONVERTERS`
  of `schema_types.py`, an array type in `_ARRAY_ELEMENT_TYPES`. Types outside `_KNOWN_TYPES` are
  resolved from `pg_type` by `CatalogResolver`: domains as their base type, arrays as `json`,
  others as `text`. Values Arrow cannot cast from text are converted in `to_castable_array` of
  `arrow_helpers.py`. Add a column to `PG_TYPES_DDL` in `tests/pg_replication/cases.py`, so
  `test_snapshot_types_match_replication` compares it with the snapshot.
- A snapshot column type that `sql_table` reflects differently: `snapshot_type_adapter` of
  `snapshot.py`.
- A pgoutput message: its decoder in `decoders.py`, handled in `MessageConsumer.process_msg`.
- A setting that formats values sent as text: `SESSION_OPTIONS` of `schema_types.py`, used by the
  replication connection and by snapshot reads.

## Seeding

Tests create their tables in a schema of their own, with a publication and a slot per test. The
credential-free tests in `test_helpers.py` and `test_arrow_helpers.py` decode pgoutput messages
captured from Postgres 17:

1. Start Postgres with logical decoding, see [Tests](#tests).
2. `uv run python -m tests.pg_replication.capture_messages` from the repository root rewrites
   `tests/pg_replication/pgoutput_messages.json` with the messages of its `SCENARIOS`.
3. Update `EXPECTED_SEGMENTS` of `test_helpers.py` when a scenario changes.

## Tests

- `test_pg_replication_source.py` needs Postgres with `wal_level=logical` and a user with the
  `REPLICATION` attribute in `sources.pg_replication.credentials`. `tests/postgres/docker-compose.yml`
  starts one. The other test modules need no Postgres.
- `test_snapshot_isolated_from_concurrent_writes` commits writes on a second connection at fixed
  points (a `threading.Barrier`) after the slot is created and before the snapshot is read: the
  snapshot must not contain them, replication must load them once.
- Whole-schema publication tests skip below Postgres 15. Other versions run in the same way, e.g.
  `docker run -p 5434:5432 -e POSTGRES_USER=loader -e POSTGRES_PASSWORD=loader -e POSTGRES_DB=dlt_data postgres:12 -c wal_level=logical`.

## Operations

- **Versions**: Postgres 10 or higher. Whole-schema publications (`table_names=None`) need
  Postgres 15. On a standby (Postgres 16+) a run ends at `pg_last_wal_replay_lsn()`.
- **Runs**: a run opens one replication connection, starts at `last_commit_lsn` of the resource
  state and stops at the first commit at or past the WAL position flushed when it started, or when
  a keepalive passes that position outside a transaction. It requests a keepalive about every
  second. A slot used by another connection is retried for 60 seconds. `add_limit(max_time=...)`
  bounds long backlogs, the next run continues.
- **Batches**: cut at a commit after `target_batch_size` changes, a transaction is never split.
  `last_commit_lsn` is set before the last item of a batch is yielded. A run that reached the end
  stores that position, so quiet publications do not hold back WAL.
- **Slot**: the position stored by a run is confirmed with a standby status update at the start of
  the next run, so the slot keeps changes until the run that loaded them succeeded. With
  `flush_slot=False` nothing is confirmed, but psycopg2 confirms keepalive positions in runs
  without changes. A slot ahead of the state, e.g. created again with `reset=True`, logs a warning
  and the run starts from the slot.
- **Types**: pgoutput sends values as text, formatted with the settings of `SESSION_OPTIONS`, and
  snapshots are read with the same settings. Column types follow `sql_table` with
  `reflection_level="full_with_precision"`, so snapshots and changes load the same columns.
  `interval` loads as text in both.
- **Numeric**: `numeric` without precision loads as dlt's default `decimal(38, 9)`. With more
  decimals the `pyarrow` backend fails (`Conversion to arrow failed for field ...`) and the `python`
  backend loads values cut to 9 decimals (seen on DuckDB). Set precision and scale with
  `columns={"<table>": {"<column>": {"data_type": "decimal", "precision": 38, "scale": 15}}}`
  in both `init_replication` and `replication_resource`. Numeric `NaN` loads as NULL with a
  warning on `pyarrow`, `Infinity` and `-Infinity` (Postgres 14+) fail the `pyarrow` cast. The
  `python` backend passes all three to the destination as `Decimal`.
- **Append**: publications without updates and deletes (`publish="insert"`) load snapshots and
  changes with the `append` write disposition, also for log tables without a primary key.
- **Primary keys**: replication takes the key from the replica identity, also compound keys.
  Without a primary key or replica identity index only `publish="insert"` works, otherwise
  `init_replication` raises before it publishes the table. With
  `REPLICA IDENTITY USING INDEX` replication merges on the index columns and snapshots on the
  primary key. Each load uses the key of its resource, both identify the same rows.
- **TOAST**: Postgres sends a value stored out of line only when it changes or when the table has
  `REPLICA IDENTITY FULL`. A merge would replace a missing value with `NULL`, so the run stops
  with `UnchangedToastException` instead. Values are not read again from the table, that read
  would see a newer row. Columns left out with `exclude_columns` or `include_columns` are not
  checked, so tables without `REPLICA IDENTITY FULL` replicate without their large columns.
- **Snapshots**: the `_dlt_<table>_s_<snapshot>` copies are not dropped. A direct snapshot holds
  the replication connection that created the slot, and with it vacuum of the whole database,
  until its last table is read, `close()` or the end of the `with` block. Tables added to a
  publication later get no snapshot.
- **Not replicated**: `TRUNCATE` logs a warning, `_dlt` tables of a schema publication are skipped.
- **Performance**: 500k rows from a local Postgres 17 into `filesystem`: 173k rows/s with
  `pyarrow`, 34k with `python`, 172k receiving messages without decoding, 23k before 1.0.0. Both
  backends peak at about 240 MB. `pyarrow` decodes inserts in chunks of `INSERT_CHUNK_SIZE`
  bytes of messages.
