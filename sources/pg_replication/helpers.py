import select
import time
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import psycopg2
from psycopg2.extensions import connection as ConnectionExt, cursor
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor

import dlt
from dlt.common import logger
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.data_writers.escape import escape_postgres_identifier
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.common.schema.typing import (
    DLT_NAME_PREFIX,
    TColumnNames,
    TTableSchemaColumns,
    TWriteDisposition,
)
from dlt.common.schema.utils import merge_column
from dlt.common.typing import TDataItem
from dlt.extract import DltResource
from dlt.sources.config import with_config
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import (
    sql_database as core_sql_database,
    sql_table as core_sql_table,
)

from .decoders import (
    UNCHANGED_TOAST,
    TupleValues,
    decode_begin,
    decode_change,
    decode_relation,
    read_tuple,
    Relation,
)
from .exceptions import (
    IncompatiblePostgresVersionException,
    NoPrimaryKeyException,
    UnchangedToastException,
)
from .schema_types import (
    _DUMMY_VALS,
    PgType,
    TValueConverter,
    add_session_options,
    get_value_converter,
    resolve_builtin_type,
    to_dlt_column_schema,
    warn_unknown_type,
)
from .snapshot import (
    ReplicationSnapshot,
    ensure_sqlalchemy_2,
    pin_session_options,
    snapshot_type_adapter,
)

__source_name__ = "pg_replication"

TBackend = Literal["python", "pyarrow"]
TInitialSnapshot = Literal["table", "unlogged_table", "direct"]

_SLOT_IN_USE_TIMEOUT = 60.0
_FEEDBACK_INTERVAL = 1.0


@with_config(
    sections=("sources", "pg_replication"),
    sections_merge_style=ConfigSectionContext.resource_merge_style,
    section_arg_name="slot_name",
)
def replication_connection(
    slot_name: str, credentials: ConnectionStringCredentials = dlt.secrets.value
) -> LogicalReplicationConnection:
    """Returns a psycopg2 LogicalReplicationConnection to interact with postgres replication functionality.

    Requires `slot_name` to be passed in order to generate compatible configuraiton section.

    Raises error if the user does not have the REPLICATION attribute assigned.
    """
    return _get_rep_conn(credentials)


@with_config(
    sections=("sources", "pg_replication"),
    sections_merge_style=ConfigSectionContext.resource_merge_style,
    section_arg_name="slot_name",
)
def init_replication(
    slot_name: str = dlt.config.value,
    pub_name: str = dlt.config.value,
    schema_name: str = dlt.config.value,
    table_names: Optional[Union[str, Sequence[str]]] = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    publish: str = "insert, update, delete",
    persist_snapshots: bool = False,
    include_columns: Optional[Mapping[str, Sequence[str]]] = None,
    exclude_columns: Optional[Mapping[str, Sequence[str]]] = None,
    columns: Optional[Mapping[str, TTableSchemaColumns]] = None,
    reset: bool = False,
    backend: TBackend = "python",
    initial_snapshot: Optional[TInitialSnapshot] = None,
) -> Optional[Union[DltResource, List[DltResource]]]:
    """Initializes replication for one, several, or all tables within a schema.

    Can be called repeatedly with the same `slot_name` and `pub_name`:
    - creates a replication slot and publication with provided names if they do not exist yet
    - skips creation of slot and publication if they already exist (unless`reset` is set to `False`)
    - supports addition of new tables by extending `table_names`
    - removing tables is not supported, i.e. exluding a table from `table_names`
      will not remove it from the publication
    - switching from a table selection to an entire schema is possible by omitting
      the `table_names` argument
    - changing `publish` has no effect (altering the published DML operations is not supported)
    - the initial snapshot can only be taken on the first call (because the snapshot
      is exported when the slot is created)

    Args:
        slot_name (str): Name of the replication slot to create if it does not exist yet.
        pub_name (str): Name of the publication to create if it does not exist yet.
        schema_name (str): Name of the schema to replicate tables from.
        table_names (Optional[Union[str, Sequence[str]]]):  Name(s) of the table(s)
          to include in the publication. If not provided, the whole schema with `schema_name` will be replicated
          (also tables added to the schema after the publication was created). You need superuser privileges
          for the whole schema replication. When specifying individual table names, the database role must
          own the tables if it's not a superuser.
        credentials (ConnectionStringCredentials): Postgres database credentials.
        publish (str): Comma-separated string of DML operations. Can be used to
          control which changes are included in the publication. Allowed operations
          are `insert`, `update`, and `delete`. `truncate` is currently not
          supported—messages of that type are ignored.
          E.g. `publish="insert"` will create a publication that only publishes insert operations.
        persist_snapshots (bool): Deprecated, same as `initial_snapshot="table"`.
        include_columns (Optional[Dict[str, Sequence[str]]]): Maps table name(s) to
          sequence of names of columns to include in the snapshot table(s).
          Any column not in the sequence is excluded. If not provided, all columns
          are included. For example:
          ```
          include_columns={
              "table_x": ["col_a", "col_c"],
              "table_y": ["col_x", "col_y", "col_z"],
          }
          ```
          Pass the same columns to `replication_resource`.
        exclude_columns (Optional[Dict[str, Sequence[str]]]): Maps table name(s) to
          sequence of names of columns to exclude from the snapshot table(s), applied after
          `include_columns`. Columns that are not included are left out of the warning about
          TOASTed values. Pass the same columns to `replication_resource`.
        columns (Optional[Dict[str, TTableSchemaColumns]]): Maps
          table name(s) to column hints to apply on the snapshot table resource(s).
          For example:
          ```
          columns={
              "table_x": {"col_a": {"data_type": "json"}},
              "table_y": {"col_y": {"precision": 32}},
          }
          ```
          Argument is only used with `initial_snapshot`.
        reset (bool): If set to True, the existing slot and publication are dropped
          and recreated. Has no effect if a slot and publication with the provided
          names do not yet exist.
        backend (TBackend): `python` yields rows, `pyarrow` yields arrow tables from the
          snapshot resources. Use the same backend as in `replication_resource`.
        initial_snapshot (Optional[TInitialSnapshot]): Returns resources that load the
          tables as they were when the replication slot was created, so the initial load and
          the replicated changes neither miss nor repeat a change:
          - `table` copies the tables into snapshot tables `_dlt_<table>_s_<snapshot>` that
            the resources read later. Drop them after the initial load.
          - `unlogged_table` copies into unlogged tables: the copy is not written to the WAL,
            but is emptied if the server crashes before the initial load.
          - `direct` reads the tables without copies. The resources must be extracted in this
            process right after `init_replication`, see `ReplicationSnapshot`.

    Returns:
        - None without `initial_snapshot`
        - a `DltResource` object or a list of `DltResource` objects for the snapshot
          table(s) of `table` and `unlogged_table` snapshots
        - a `ReplicationSnapshot` list of resources for `direct` snapshots
    """
    if isinstance(table_names, str):
        table_names = [table_names]
    if persist_snapshots and initial_snapshot is None:
        initial_snapshot = "table"
    if initial_snapshot == "direct":
        ensure_sqlalchemy_2()

    rep_conn = _get_rep_conn(credentials)
    keep_rep_conn = False
    try:
        with rep_conn.cursor() as cur:
            if reset:
                drop_replication_slot(slot_name, cur)
                drop_publication(pub_name, cur)
            create_publication(pub_name, cur, publish)
            pub_ops = get_pub_ops(pub_name, cur)
            if pub_ops["update"] or pub_ops["delete"]:
                # checked before the tables are published, Postgres would reject their updates
                tables_without_key = get_tables_without_key(
                    schema_name, table_names, cur
                )
                if tables_without_key:
                    raise NoPrimaryKeyException(schema_name, tables_without_key)
            if table_names is None:
                add_schema_to_publication(schema_name, pub_name, cur)
            else:
                add_tables_to_publication(table_names, schema_name, pub_name, cur)
            if pub_ops["update"]:
                warn_unchanged_toast(pub_name, cur, include_columns, exclude_columns)

            # no commands may run on `rep_conn` after the slot is created, they would end the
            # exported snapshot
            slot = create_replication_slot(slot_name, cur)
        if initial_snapshot is None:
            return None
        if slot is None:
            raise RuntimeError(
                f"Cannot create snapshots because slot {slot_name} is already created."
            )

        # get list of tables via sql_database if not provided
        if table_names is None:
            # do not include dlt tables
            table_names = [
                table_name
                for table_name in core_sql_database(
                    credentials, schema=schema_name, reflection_level="minimal"
                ).resources.keys()
                if not table_name.lower().startswith(DLT_NAME_PREFIX)
            ]
        write_disposition: TWriteDisposition = (
            "merge" if pub_ops["update"] or pub_ops["delete"] else "append"
        )

        if initial_snapshot == "direct":
            snapshot = ReplicationSnapshot(rep_conn, slot["snapshot_name"])
            keep_rep_conn = True
            for table_name in table_names:
                resource = core_sql_table(
                    credentials=credentials,
                    table=table_name,
                    schema=schema_name,
                    reflection_level="full_with_precision",
                    backend="pyarrow" if backend == "pyarrow" else "sqlalchemy",
                    included_columns=_columns_of_table(include_columns, table_name),
                    excluded_columns=_columns_of_table(exclude_columns, table_name),
                    type_adapter_callback=snapshot.type_adapter_callback,
                    engine_adapter_callback=snapshot.engine_adapter_callback,
                    table_loader_class=snapshot.table_loader_class(),
                )
                resource.apply_hints(
                    write_disposition=write_disposition,
                    columns=None if columns is None else columns.get(table_name),
                )
                snapshot.append(resource)
            return snapshot

        # need separate session to read the snapshot: https://stackoverflow.com/q/75852587
        with closing(_get_conn(credentials)) as conn:
            with conn.cursor() as cur_snap:
                # set the snapshot to the snaphost of the newly created replication slot
                cur_snap.execute(
                    "START TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
                    " SET TRANSACTION SNAPSHOT %s;",
                    (slot["snapshot_name"],),
                )
                snapshot_tables = [
                    (
                        table_name,
                        persist_snapshot_table(
                            snapshot_name=slot["snapshot_name"],
                            table_name=table_name,
                            schema_name=schema_name,
                            cur=cur_snap,
                            include_columns=_columns_of_table(
                                include_columns, table_name
                            ),
                            exclude_columns=_columns_of_table(
                                exclude_columns, table_name
                            ),
                            unlogged=initial_snapshot == "unlogged_table",
                        ),
                        _get_pk(cur_snap, table_name, schema_name),
                    )
                    for table_name in table_names
                ]
            # commit all tables before creating resources that will reflect them
            conn.commit()
        snapshot_table_resources = [
            snapshot_table_resource(
                snapshot_table_name=snapshot_table_name,
                schema_name=schema_name,
                table_name=table_name,
                write_disposition=write_disposition,
                columns=None if columns is None else columns.get(table_name),
                primary_key=primary_key,
                credentials=credentials,
                backend=backend,
            )
            for table_name, snapshot_table_name, primary_key in snapshot_tables
        ]
        if len(snapshot_table_resources) == 1:
            return snapshot_table_resources[0]
        return snapshot_table_resources
    finally:
        if not keep_rep_conn:
            rep_conn.close()


@dlt.sources.config.with_config(sections=("sources", "pg_replication"))
def get_pg_version(
    cur: cursor = None,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
) -> int:
    """Returns Postgres server version as int."""
    if cur is not None:
        return cur.connection.server_version
    with closing(_get_conn(credentials)) as conn:
        return conn.server_version


def create_publication(
    name: str,
    cur: cursor,
    publish: str = "insert, update, delete",
) -> None:
    """Creates a publication for logical replication if it doesn't exist yet.

    Does nothing if the publication already exists.
    Raises error if the user does not have the CREATE privilege for the database.
    """
    esc_name = escape_postgres_identifier(name)
    try:
        cur.execute(f"CREATE PUBLICATION {esc_name} WITH (publish = %s);", (publish,))
        logger.info(
            f"Successfully created publication {esc_name} with publish = '{publish}'."
        )
    except psycopg2.errors.DuplicateObject:  # the publication already exists
        logger.info(f'Publication "{name}" already exists.')


def add_table_to_publication(
    table_name: str,
    schema_name: str,
    pub_name: str,
    cur: cursor,
) -> None:
    """Adds a table to a publication for logical replication.

    Does nothing if the table is already a member of the publication.
    Raises error if the user is not owner of the table.
    """
    qual_name = _make_qualified_table_name(table_name, schema_name)
    esc_pub_name = escape_postgres_identifier(pub_name)
    try:
        cur.execute(f"ALTER PUBLICATION {esc_pub_name} ADD TABLE {qual_name};")
        logger.info(
            f"Successfully added table {qual_name} to publication {esc_pub_name}."
        )
    except psycopg2.errors.DuplicateObject:
        logger.info(
            f"Table {qual_name} is already a member of publication {esc_pub_name}."
        )


def add_tables_to_publication(
    table_names: Union[str, Sequence[str]],
    schema_name: str,
    pub_name: str,
    cur: cursor,
) -> None:
    """Adds one or multiple tables to a publication for logical replication.

    Calls `add_table_to_publication` for each table in `table_names`.
    """
    if isinstance(table_names, str):
        table_names = [table_names]
    for table_name in table_names:
        add_table_to_publication(table_name, schema_name, pub_name, cur)


def add_schema_to_publication(
    schema_name: str,
    pub_name: str,
    cur: cursor,
) -> None:
    """Adds a schema to a publication for logical replication if the schema is not a member yet.

    Raises error if the user is not a superuser.
    """
    if (version := get_pg_version(cur)) < 150000:
        raise IncompatiblePostgresVersionException(
            f"Cannot add schema to publication because the Postgres server version {version} is"
            " too low. Adding schemas to a publication is only supported for Postgres version 15"
            " or higher. Upgrade your Postgres server version or set the `table_names` argument"
            " to explicitly specify table names."
        )
    esc_schema_name = escape_postgres_identifier(schema_name)
    esc_pub_name = escape_postgres_identifier(pub_name)
    try:
        cur.execute(
            f"ALTER PUBLICATION {esc_pub_name} ADD TABLES IN SCHEMA {esc_schema_name};"
        )
        logger.info(
            f"Successfully added schema {esc_schema_name} to publication {esc_pub_name}."
        )
    except psycopg2.errors.DuplicateObject:
        logger.info(
            f"Schema {esc_schema_name} is already a member of publication {esc_pub_name}."
        )


def create_replication_slot(  # type: ignore[return]
    name: str, cur: ReplicationCursor, output_plugin: str = "pgoutput"
) -> Optional[Dict[str, str]]:
    """Creates a replication slot if it doesn't exist yet."""
    try:
        cur.create_replication_slot(name, output_plugin=output_plugin)
        logger.info(f'Successfully created replication slot "{name}".')
        result = cur.fetchone()
        return {
            "slot_name": result[0],
            "consistent_point": result[1],
            "snapshot_name": result[2],
            "output_plugin": result[3],
        }
    except psycopg2.errors.DuplicateObject:  # the replication slot already exists
        logger.info(
            f'Replication slot "{name}" cannot be created because it already exists.'
        )


def drop_replication_slot(name: str, cur: ReplicationCursor) -> None:
    """Drops a replication slot if it exists."""
    try:
        cur.drop_replication_slot(name)
        logger.info(f'Successfully dropped replication slot "{name}".')
    except psycopg2.errors.UndefinedObject:  # the replication slot does not exist
        logger.info(
            f'Replication slot "{name}" cannot be dropped because it does not exist.'
        )


def drop_publication(name: str, cur: ReplicationCursor) -> None:
    """Drops a publication if it exists."""
    esc_name = escape_postgres_identifier(name)
    try:
        cur.execute(f"DROP PUBLICATION {esc_name};")
        logger.info(f"Successfully dropped publication {esc_name}.")
    except psycopg2.errors.UndefinedObject:  # the publication does not exist
        logger.info(
            f"Publication {esc_name} cannot be dropped because it does not exist."
        )


def persist_snapshot_table(
    snapshot_name: str,
    table_name: str,
    schema_name: str,
    cur: cursor,
    include_columns: Optional[Sequence[str]] = None,
    exclude_columns: Optional[Sequence[str]] = None,
    unlogged: bool = False,
) -> str:
    """Persists exported snapshot table state.

    Copies the table into a new table. An unlogged copy is not written to WAL, but is emptied
    if the server crashes. Interval columns are copied as text because they have no dlt data type.
    """
    qual_name = _make_qualified_table_name(table_name, schema_name)
    cur.execute(
        "SELECT attname, atttypid FROM pg_attribute WHERE attrelid = %s::regclass"
        " AND attnum > 0 AND NOT attisdropped ORDER BY attnum;",
        (qual_name,),
    )
    select_list = []
    for column_name, type_id in cur.fetchall():
        if not _is_column_included(column_name, include_columns, exclude_columns):
            continue
        esc_name = escape_postgres_identifier(column_name)
        if type_id in (1186, 1187):  # interval, interval[]
            select_list.append(f"{esc_name}::text AS {esc_name}")
        else:
            select_list.append(esc_name)
    # make sure to shorten identifier
    naming = NamingConvention(63)
    # name must start with _dlt so we skip this table when replicating
    snapshot_table_name = naming.normalize_table_identifier(
        f"_dlt_{table_name}_s_{snapshot_name}"
    )
    snapshot_qual_name = _make_qualified_table_name(snapshot_table_name, schema_name)
    cur.execute(
        f"CREATE {'UNLOGGED ' if unlogged else ''}TABLE {snapshot_qual_name}"
        f" AS SELECT {', '.join(select_list)}"
        f" FROM {qual_name};"
    )
    logger.info(f"Successfully persisted snapshot table state in {snapshot_qual_name}.")
    return snapshot_table_name


def snapshot_table_resource(
    snapshot_table_name: str,
    schema_name: str,
    table_name: str,
    primary_key: TColumnNames,
    write_disposition: TWriteDisposition,
    columns: TTableSchemaColumns = None,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    backend: TBackend = "python",
) -> DltResource:
    """Returns a resource for a persisted snapshot table.

    Can be used to perform an initial load of the table, so all data that
    existed in the table prior to initializing replication is also captured.
    """
    resource: DltResource = core_sql_table(
        credentials=credentials,
        table=snapshot_table_name,
        schema=schema_name,
        reflection_level="full_with_precision",
        backend="pyarrow" if backend == "pyarrow" else "sqlalchemy",
        type_adapter_callback=snapshot_type_adapter,
        engine_adapter_callback=pin_session_options,
    )
    resource.apply_hints(
        table_name=table_name,
        write_disposition=write_disposition,
        columns=columns,
        primary_key=primary_key,
    )
    return resource


def get_pub_ops(
    pub_name: str,
    cur: cursor,
) -> Dict[str, bool]:
    """Returns dictionary of DML operations and their publish status."""
    cur.execute(
        "SELECT pubinsert, pubupdate, pubdelete FROM pg_publication WHERE pubname = %s;",
        (pub_name,),
    )
    result = cur.fetchone()
    if result is None:
        raise ValueError(f'Publication "{pub_name}" does not exist.')
    return {
        "insert": result[0],
        "update": result[1],
        "delete": result[2],
    }


def get_tables_without_key(
    schema_name: str, table_names: Optional[Sequence[str]], cur: cursor
) -> List[str]:
    """Returns tables of `table_names`, or of the whole schema, without a primary key or replica
    identity index. `_dlt` tables are skipped.
    """
    names = None if table_names is None else list(table_names)
    cur.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'p')
          AND (%s::text[] IS NULL OR c.relname = ANY(%s::text[]))
          AND NOT EXISTS (
            SELECT 1 FROM pg_index i
            WHERE i.indrelid = c.oid AND CASE c.relreplident
              WHEN 'i' THEN i.indisreplident WHEN 'n' THEN false ELSE i.indisprimary END
          )
        ORDER BY c.relname;
        """,
        (schema_name, names, names),
    )
    return [
        name
        for (name,) in cur.fetchall()
        if not name.lower().startswith(DLT_NAME_PREFIX)
    ]


def warn_unchanged_toast(
    pub_name: str,
    cur: cursor,
    include_columns: Optional[Mapping[str, Sequence[str]]] = None,
    exclude_columns: Optional[Mapping[str, Sequence[str]]] = None,
) -> None:
    """Warns about included columns of published tables whose updates may not send unchanged
    TOASTed values.

    Such values are sent only with REPLICA IDENTITY FULL, see `UnchangedToastException`.
    """
    cur.execute(
        """
        SELECT pt.schemaname, pt.tablename, array_agg(a.attname::text ORDER BY a.attnum)
        FROM pg_publication_tables pt
        JOIN pg_namespace n ON n.nspname = pt.schemaname
        JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        WHERE pt.pubname = %s AND c.relreplident <> 'f' AND a.attlen = -1
          AND a.attstorage <> 'p'
        GROUP BY pt.schemaname, pt.tablename;
        """,
        (pub_name,),
    )
    for schema_name, table_name, column_names in cur.fetchall():
        include = _columns_of_table(include_columns, table_name)
        exclude = _columns_of_table(exclude_columns, table_name)
        column_names = [
            c for c in column_names if _is_column_included(c, include, exclude)
        ]
        if not column_names:
            continue
        logger.warning(
            f"Table {schema_name}.{table_name} does not have REPLICA IDENTITY FULL and columns"
            f" {', '.join(column_names)} may store large values out of line. An update that does"
            " not change such a value stops the replication. Run `ALTER TABLE"
            f" {schema_name}.{table_name} REPLICA IDENTITY FULL` or exclude the columns with"
            " `exclude_columns` to prevent it."
        )


def get_confirmed_flush_lsn(slot_name: str, cur: cursor) -> int:
    """Returns the position up to which the slot consumer confirmed changes."""
    cur.execute(
        "SELECT confirmed_flush_lsn - '0/0' FROM pg_replication_slots WHERE slot_name = %s;",
        (slot_name,),
    )
    result = cur.fetchone()
    if result is None:
        raise ValueError(f'Replication slot "{slot_name}" does not exist.')
    return int(result[0] or 0)


def get_current_lsn(cur: cursor) -> int:
    """Returns the WAL position flushed on the server, replayed position on a standby."""
    cur.execute(
        "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()"
        " ELSE pg_current_wal_flush_lsn() END - '0/0';"
    )
    return int(cur.fetchone()[0])


def lsn_int_to_hex(lsn: int) -> str:
    """Convert integer LSN to postgres' hexadecimal representation."""
    # https://stackoverflow.com/questions/66797767/lsn-external-representation.
    return f"{lsn >> 32 & 4294967295:X}/{lsn & 4294967295:08X}"


def _get_conn(
    credentials: ConnectionStringCredentials,
    connection_factory: Optional[Any] = None,
) -> ConnectionExt:
    """Returns a psycopg2 connection to interact with postgres."""
    conn = psycopg2.connect(
        dsn=credentials.to_native_representation(),
        connection_factory=connection_factory,
        options=add_session_options((credentials.query or {}).get("options")),
    )
    if connection_factory is not LogicalReplicationConnection:
        conn.autocommit = False
    return conn  # type: ignore[no-any-return]


def _get_rep_conn(
    credentials: ConnectionStringCredentials,
) -> LogicalReplicationConnection:
    """Returns a psycopg2 LogicalReplicationConnection to interact with postgres replication functionality.

    Raises error if the user does not have the REPLICATION attribute assigned.
    """
    return _get_conn(credentials, LogicalReplicationConnection)  # type: ignore[return-value]


def _make_qualified_table_name(table_name: str, schema_name: str) -> str:
    """Escapes and combines a schema and table name."""
    return (
        escape_postgres_identifier(schema_name)
        + "."
        + escape_postgres_identifier(table_name)
    )


def _columns_of_table(
    columns: Optional[Mapping[str, Sequence[str]]], table_name: str
) -> Optional[List[str]]:
    """Returns the column names of `include_columns` or `exclude_columns` for `table_name`."""
    if columns is None or table_name not in columns:
        return None
    return list(columns[table_name])


def _is_column_included(
    column_name: str,
    include_columns: Optional[Sequence[str]],
    exclude_columns: Optional[Sequence[str]],
) -> bool:
    return (include_columns is None or column_name in include_columns) and (
        exclude_columns is None or column_name not in exclude_columns
    )


def _get_pk(
    cur: cursor,
    table_name: str,
    schema_name: str,
) -> Optional[TColumnNames]:
    """Returns primary key column(s) for postgres table.

    Returns None if no primary key columns exist.
    """
    qual_name = _make_qualified_table_name(table_name, schema_name)
    # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    cur.execute(
        """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = %s::regclass
        AND    i.indisprimary;
    """,
        (qual_name,),
    )
    result = [tup[0] for tup in cur.fetchall()]
    if len(result) == 0:
        return None
    elif len(result) == 1:
        return result[0]  # type: ignore[no-any-return]
    return result


class CatalogResolver:
    """Looks up types and primary keys in the source database."""

    def __init__(self, conn: ConnectionExt) -> None:
        self.conn = conn
        self._types: Dict[int, PgType] = {}

    def resolve_type(self, type_id: int, atttypmod: int) -> PgType:
        """Resolves domains to their base types and arrays to their element types."""
        if pg_type := resolve_builtin_type(type_id, atttypmod):
            return pg_type
        if type_id not in self._types:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT typname, typtype, typbasetype, typtypmod, typcategory, typelem"
                    " FROM pg_type WHERE oid = %s;",
                    (type_id,),
                )
                (
                    name,
                    typtype,
                    base_type_id,
                    base_typmod,
                    category,
                    element_type_id,
                ) = cur.fetchone()
            if typtype == "d":
                self._types[type_id] = self.resolve_type(base_type_id, base_typmod)
            elif category == "A" and element_type_id:
                element = self.resolve_type(element_type_id, -1)
                self._types[type_id] = PgType(type_id, atttypmod, element.type_id)
            else:
                if typtype != "e":
                    warn_unknown_type(type_id, name)
                self._types[type_id] = PgType(type_id, atttypmod)
        pg_type = self._types[type_id]
        if pg_type.atttypmod == -1 and atttypmod != -1:
            return pg_type._replace(atttypmod=atttypmod)
        return pg_type

    def get_primary_key(self, relation_id: int) -> List[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid"
                " AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s AND i.indisprimary;",
                (relation_id,),
            )
            return [row[0] for row in cur.fetchall()]


@dataclass
class RelationSchema:
    """Table schema received in a Relation message, with dlt hints for the table."""

    relation_id: int
    schema_name: str
    table_name: str
    columns: List[Any]
    pg_types: List[PgType]
    key_indices: List[int]
    included_indices: List[int]
    column_schemas: TTableSchemaColumns
    write_disposition: TWriteDisposition
    skip: bool = False


@dataclass
class Segment:
    """Changes of a single table with the same schema.

    Inserts are kept as raw messages and decoded when the batch is converted, other
    changes as decoded tuples.
    """

    relation: RelationSchema
    insert_payloads: List[bytes] = field(default_factory=list)
    insert_lsns: List[int] = field(default_factory=list)
    rows: List[TupleValues] = field(default_factory=list)
    row_lsns: List[int] = field(default_factory=list)
    row_deleted_ts: List[Optional[datetime]] = field(default_factory=list)


def _default_resolve_type(type_id: int, atttypmod: int) -> PgType:
    return resolve_builtin_type(type_id, atttypmod) or PgType(type_id, atttypmod)


class MessageConsumer:
    """Decodes pgoutput messages into segments of changes, one per table and table schema.

    Changes of a transaction are never split between batches: a batch is ready after a commit
    when it holds at least `target_batch_size` changes.
    """

    def __init__(
        self,
        pub_ops: Dict[str, bool],
        upto_lsn: int,
        target_batch_size: int = 1000,
        include_columns: Optional[Mapping[str, Sequence[str]]] = None,
        exclude_columns: Optional[Mapping[str, Sequence[str]]] = None,
        columns: Optional[Mapping[str, TTableSchemaColumns]] = None,
        resolve_type: Callable[[int, int], PgType] = _default_resolve_type,
        resolve_primary_key: Optional[Callable[[int], List[str]]] = None,
    ) -> None:
        self.pub_ops = pub_ops
        self.upto_lsn = upto_lsn
        self.target_batch_size = target_batch_size
        self.include_columns = include_columns
        self.exclude_columns = exclude_columns
        self.columns = columns
        self.resolve_type = resolve_type
        self.resolve_primary_key = resolve_primary_key

        self.relations: Dict[int, RelationSchema] = {}
        self.segments: List[Segment] = []
        self.open_segments: Dict[int, Segment] = {}
        self.n_items = 0
        self.in_transaction = False
        self.commit_ts: Optional[datetime] = None
        self.last_commit_lsn: Optional[int] = None
        self.batch_ready = False
        self.consumed_all = False

    def process_msg(self, payload: bytes, data_start: int) -> None:
        op = payload[0]
        if op == 73:  # I
            relation = self.relations[int.from_bytes(payload[1:5], "big")]
            if not relation.skip:
                segment = self._get_segment(relation)
                segment.insert_payloads.append(payload)
                segment.insert_lsns.append(data_start)
                self.n_items += 1
        elif op in (85, 68):  # U, D
            self.process_change(payload, data_start)
        elif op == 66:  # B
            self.in_transaction = True
            self.commit_ts = decode_begin(payload).commit_ts
        elif op == 67:  # C
            self.in_transaction = False
            self.last_commit_lsn = data_start
            if data_start >= self.upto_lsn:
                self.consumed_all = True
            if self.n_items >= self.target_batch_size:
                self.batch_ready = True
        elif op == 82:  # R
            self.process_relation(decode_relation(payload))
        elif op == 84:  # T
            logger.warning(
                "The truncate operation is currently not supported. "
                "Truncate replication messages are ignored."
            )
        elif op not in (89, 79, 77):  # Y(type), O(origin), M(logical message)
            raise ValueError(f"Unknown replication op {op}")

    def process_relation(self, relation: Relation) -> None:
        # changes received from now on have the new table schema
        self.open_segments.pop(relation.relation_id, None)
        table_name = relation.relation_name
        skip = table_name.lower().startswith(DLT_NAME_PREFIX)
        pg_types = [self.resolve_type(c.type_id, c.atttypmod) for c in relation.columns]
        if relation.replica_identity == "f" and self.resolve_primary_key:
            # with REPLICA IDENTITY FULL all columns are flagged as key
            pk_names = set(self.resolve_primary_key(relation.relation_id))
            key_indices = [
                i for i, c in enumerate(relation.columns) if c.name in pk_names
            ]
        else:
            key_indices = [i for i, c in enumerate(relation.columns) if c.part_of_pkey]
        if (
            not skip
            and not key_indices
            and (self.pub_ops["update"] or self.pub_ops["delete"])
        ):
            raise NoPrimaryKeyException(relation.namespace, [table_name])
        include = _columns_of_table(self.include_columns, table_name)
        exclude = _columns_of_table(self.exclude_columns, table_name)
        included_indices = [
            i
            for i, c in enumerate(relation.columns)
            if _is_column_included(c.name, include, exclude)
        ]

        column_schemas: TTableSchemaColumns = {
            relation.columns[i].name: to_dlt_column_schema(
                relation.columns[i], pg_types[i], i in key_indices
            )
            for i in included_indices
        }
        hints = {} if self.columns is None else self.columns.get(table_name, {})
        for column_name, column_hint in hints.items():
            if column_name in column_schemas:
                column_schemas[column_name] = merge_column(
                    column_schemas[column_name], column_hint
                )
            else:
                column_schemas[column_name] = {"name": column_name, **column_hint}

        # add hints for replication columns
        column_schemas["lsn"] = {"name": "lsn", "data_type": "bigint", "nullable": True}
        if self.pub_ops["update"] or self.pub_ops["delete"]:
            column_schemas["lsn"]["dedup_sort"] = "desc"
        if self.pub_ops["delete"]:
            column_schemas["deleted_ts"] = {
                "name": "deleted_ts",
                "hard_delete": True,
                "data_type": "timestamp",
                "nullable": True,
            }
        write_disposition: TWriteDisposition = "append"
        if self.pub_ops["update"] or self.pub_ops["delete"]:
            write_disposition = "merge"

        self.relations[relation.relation_id] = RelationSchema(
            relation_id=relation.relation_id,
            schema_name=relation.namespace,
            table_name=table_name,
            columns=relation.columns,
            pg_types=pg_types,
            key_indices=key_indices,
            included_indices=included_indices,
            column_schemas=column_schemas,
            write_disposition=write_disposition,
            skip=skip,
        )

    def process_change(self, payload: bytes, data_start: int) -> None:
        change = decode_change(payload)
        relation = self.relations[change.relation_id]
        if relation.skip:
            return
        segment = self._get_segment(relation)
        if change.op == "D":
            self._add_row(segment, change.old_values, data_start, self.commit_ts)
            return
        new_values = change.new_values
        if change.old_values is not None:
            old_values = change.old_values
            if any(old_values[i] != new_values[i] for i in relation.key_indices):
                # primary key changed: delete the row under the old key
                self._add_row(segment, list(old_values), data_start, self.commit_ts)
            if change.old_kind == "O":
                # with REPLICA IDENTITY FULL the old row has the unchanged TOASTed values
                new_values = [
                    old if new is UNCHANGED_TOAST else new
                    for old, new in zip(old_values, new_values)
                ]
        if UNCHANGED_TOAST in new_values:
            missing = [
                relation.columns[i].name
                for i in relation.included_indices
                if new_values[i] is UNCHANGED_TOAST
            ]
            if missing:
                raise UnchangedToastException(
                    relation.schema_name, relation.table_name, missing
                )
        self._add_row(segment, new_values, data_start, None)

    def take_batch(self) -> List[Segment]:
        segments = self.segments
        self.segments = []
        self.open_segments = {}
        self.n_items = 0
        self.batch_ready = False
        return segments

    def _get_segment(self, relation: RelationSchema) -> Segment:
        segment = self.open_segments.get(relation.relation_id)
        if segment is None:
            segment = self.open_segments[relation.relation_id] = Segment(relation)
            self.segments.append(segment)
        return segment

    def _add_row(
        self,
        segment: Segment,
        values: TupleValues,
        lsn: int,
        deleted_ts: Optional[datetime],
    ) -> None:
        segment.rows.append(values)
        segment.row_lsns.append(lsn)
        segment.row_deleted_ts.append(deleted_ts)
        self.n_items += 1


def segment_hints(relation: RelationSchema) -> TDataItem:
    """Returns a meta item that sets the table hints of the segment."""
    return dlt.mark.with_hints(
        [],
        dlt.mark.make_hints(
            table_name=relation.table_name,
            write_disposition=relation.write_disposition,
            columns=relation.column_schemas,
        ),
        create_table_variant=True,
    )


def segment_to_rows(segment: Segment) -> List[TDataItem]:
    """Converts changes in a segment to rows (dicts)."""
    relation = segment.relation
    indices = relation.included_indices
    names = [relation.columns[i].name for i in indices]
    converters: List[Optional[TValueConverter]] = [
        get_value_converter(relation.pg_types[i]) for i in indices
    ]
    dummies = [
        _DUMMY_VALS[relation.column_schemas[name].get("data_type", "text")]
        for name in names
    ]
    with_deletes = "deleted_ts" in relation.column_schemas
    rows: List[TDataItem] = []

    def _to_row(values: TupleValues) -> Dict[str, Any]:
        return {
            name: value if value is None or convert is None else convert(value)
            for name, convert, value in zip(
                names, converters, [values[i] for i in indices]
            )
        }

    for payload, lsn in zip(segment.insert_payloads, segment.insert_lsns):
        row = _to_row(read_tuple(payload, 6)[0])
        row["lsn"] = lsn
        rows.append(row)
    for values, lsn, deleted_ts in zip(
        segment.rows, segment.row_lsns, segment.row_deleted_ts
    ):
        row = _to_row(values)
        row["lsn"] = lsn
        if deleted_ts is not None:
            # replace NULLs to prevent NOT NULL violations in staging table
            for name, dummy in zip(names, dummies):
                if row[name] is None:
                    row[name] = dummy
            row["deleted_ts"] = deleted_ts
        elif with_deletes:
            row["deleted_ts"] = None
        rows.append(row)
    return rows


class ChangesBatch(NamedTuple):
    items: List[TDataItem]
    last_commit_lsn: int


def stream_changes(
    credentials: ConnectionStringCredentials,
    slot_name: str,
    pub_name: str,
    start_lsn: int = 0,
    target_batch_size: int = 1000,
    include_columns: Optional[Mapping[str, Sequence[str]]] = None,
    exclude_columns: Optional[Mapping[str, Sequence[str]]] = None,
    columns: Optional[Mapping[str, TTableSchemaColumns]] = None,
    flush_slot: bool = True,
    backend: TBackend = "python",
) -> Iterator[ChangesBatch]:
    """Streams changes from the slot until the WAL position flushed at the start is reached.

    Uses a single replication connection. With `flush_slot`, the slot is confirmed up to
    `start_lsn` at the start, i.e. changes of the previous run are discarded only when a
    pipeline run with the state containing `start_lsn` succeeded. The last batch has the
    position up to which all changes were received, also when none were published.
    """
    if backend == "pyarrow":
        from .arrow_helpers import segment_to_arrow

        segment_to_data = segment_to_arrow
    else:
        segment_to_data = segment_to_rows

    conn = _get_conn(credentials)
    conn.autocommit = True
    rep_cur: Optional[ReplicationCursor] = None
    try:
        with conn.cursor() as cur:
            confirmed_flush_lsn = get_confirmed_flush_lsn(slot_name, cur)
            upto_lsn = get_current_lsn(cur)
            pub_ops = get_pub_ops(pub_name, cur)
        if start_lsn and start_lsn < confirmed_flush_lsn:
            logger.warning(
                f"Replication slot {slot_name} confirmed changes up to"
                f" {lsn_int_to_hex(confirmed_flush_lsn)}, past {lsn_int_to_hex(start_lsn)} stored"
                " in the pipeline state. Expected after the slot was re-created ie. with"
                " `init_replication(reset=True)`, otherwise changes in between are lost."
            )
        logger.info(
            f"Replicating slot {slot_name} publication {pub_name} from"
            f" {lsn_int_to_hex(max(start_lsn, confirmed_flush_lsn))} to {lsn_int_to_hex(upto_lsn)}"
        )
        catalog = CatalogResolver(conn)
        consumer = MessageConsumer(
            pub_ops=pub_ops,
            upto_lsn=upto_lsn,
            target_batch_size=target_batch_size,
            include_columns=include_columns,
            exclude_columns=exclude_columns,
            columns=columns,
            resolve_type=catalog.resolve_type,
            resolve_primary_key=catalog.get_primary_key,
        )

        def _batch(lsn: int) -> ChangesBatch:
            items: List[TDataItem] = []
            segments = consumer.take_batch()
            while segments:
                # converted segments are released so raw changes and data do not pile up
                segment = segments.pop(0)
                items.append(segment_hints(segment.relation))
                items.append(
                    dlt.mark.with_table_name(
                        segment_to_data(segment), segment.relation.table_name
                    )
                )
            return ChangesBatch(items, lsn)

        rep_cur = _start_replication(
            credentials, slot_name, pub_name, start_lsn, flush_slot
        )
        last_feedback = 0.0
        while not consumer.consumed_all:
            msg = rep_cur.read_message()
            if msg is not None:
                consumer.process_msg(msg.payload, msg.data_start)
                if consumer.batch_ready and not consumer.consumed_all:
                    yield _batch(consumer.last_commit_lsn)
                    # yielding may take long, let the server know the client is alive
                    rep_cur.send_feedback(force=True)
                continue
            if not consumer.in_transaction and rep_cur.wal_end >= upto_lsn:
                break
            if time.monotonic() - last_feedback > _FEEDBACK_INTERVAL:
                # a reply carries the server WAL position, also when nothing is published
                rep_cur.send_feedback(reply=True, force=True)
                last_feedback = time.monotonic()
            select.select([rep_cur], [], [], _FEEDBACK_INTERVAL)
        yield _batch(max(start_lsn, consumer.last_commit_lsn or 0, upto_lsn))
    finally:
        if rep_cur is not None:
            rep_cur.connection.close()
        conn.close()


def _start_replication(
    credentials: ConnectionStringCredentials,
    slot_name: str,
    pub_name: str,
    start_lsn: int,
    flush_slot: bool,
) -> ReplicationCursor:
    """Starts replication, retries while the slot is still used by a previous connection."""
    started_at = time.monotonic()
    while True:
        rep_conn = _get_rep_conn(credentials)
        try:
            rep_cur: ReplicationCursor = rep_conn.cursor()
            rep_cur.start_replication(
                slot_name=slot_name,
                start_lsn=start_lsn,
                decode=False,
                options={"publication_names": pub_name, "proto_version": "1"},
            )
            if flush_slot and start_lsn:
                rep_cur.send_feedback(flush_lsn=start_lsn, force=True)
            return rep_cur
        except psycopg2.errors.ObjectInUse:
            rep_conn.close()
            if time.monotonic() - started_at > _SLOT_IN_USE_TIMEOUT:
                raise
            logger.info(f"Replication slot {slot_name} is in use, retrying.")
            time.sleep(1.0)
        except Exception:
            rep_conn.close()
            raise
