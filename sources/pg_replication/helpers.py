import select
import time
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import psycopg2
from psycopg2.extensions import connection as ConnectionExt, cursor
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor

import dlt
from dlt.common import logger
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.data_writers.escape import escape_postgres_identifier
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.exceptions import MissingDependencyException
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.common.pipeline import PipelineContext
from dlt.common.schema import Schema
from dlt.common.schema.typing import (
    DLT_NAME_PREFIX,
    TColumnNames,
    TColumnSchema,
    TTableSchemaColumns,
    TWriteDisposition,
)
from dlt.common.schema.utils import merge_column
from dlt.common.typing import TDataItem
from dlt.common.utils import uniq_id
from dlt.extract import DltResource
from dlt.extract.source import SourceSchemaInjectableContext
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
    ChangePosition,
    ColumnNameCollisionException,
    IncompatibleColumnTypeException,
    IncompatiblePostgresVersionException,
    NoPrimaryKeyException,
    NullInNotNullColumnException,
    PublicationPrivilegesException,
    SnapshotSchemaNotFoundException,
    TableNameCollisionException,
    UnchangedToastException,
    ValueConversionException,
)
from .schema_types import (
    _DUMMY_VALS,
    COLUMN_TYPE_HINTS,
    DELETED_TS,
    LEGACY_DELETED_TS,
    LEGACY_LSN,
    LSN,
    PgType,
    TReflectionLevel,
    TValueConverter,
    add_session_options,
    column_type_fits,
    format_column_type,
    get_value_converter,
    resolve_builtin_type,
    to_dlt_column_schema,
    to_dlt_column_type,
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
    reflection_level: TReflectionLevel = "full",
    reflect_not_null: bool = False,
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
    - the snapshot of the slot is exported only when the slot is created, on an existing slot
      `initial_snapshot` reads the tables as they are, see `replication_slot_exists`

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
          snapshot resources. Use the same backend as in `replication_resource`: arrow tables
          get no `_dlt_load_id` and `_dlt_id` columns, rows of `python` add them as NOT NULL
          columns, so changes of one backend fail to load into a table loaded with the other.
        initial_snapshot (Optional[TInitialSnapshot]): Returns resources that load the
          tables as they were when the replication slot was created, so the initial load and
          the replicated changes neither miss nor repeat a change:
          - `table` copies the tables into snapshot tables `_dlt_<table>_s_<snapshot>` that
            the resources read later. Drop them after the initial load.
          - `unlogged_table` copies into unlogged tables: the copy is not written to the WAL,
            but is emptied if the server crashes before the initial load.
          - `direct` reads the tables without copies. The resources must be extracted in this
            process right after `init_replication`, see `ReplicationSnapshot`.
          On an existing slot the tables are loaded as they are when read and
          `replication_resource` merges the replayed changes over them, so the publication must
          publish updates or deletes. Pass `initial_snapshot` only for the tables you want
          loaded, ie. all tables when `replication_slot_exists` is False, later the added ones.
        reflection_level (TReflectionLevel): `full` creates snapshot columns without length
          and integer size, so later changes of those fit, `full_with_precision` creates them
          as in the source. Numeric precision and scale are kept on both levels. Use the same
          level as in `replication_resource`.
        reflect_not_null (bool): Creates NOT NULL snapshot columns for NOT NULL source columns.
          By default only primary key columns are NOT NULL, so renamed, dropped or excluded
          columns can be NULL in later changes. `replication_resource` stops when a NOT NULL
          destination column would get NULL.

    Returns:
        - None without `initial_snapshot`
        - a `DltResource` object or a list of `DltResource` objects for the snapshot
          table(s) of `table` and `unlogged_table` snapshots
        - a `ReplicationSnapshot` list of resources for `direct` snapshots
    """
    if isinstance(table_names, str):
        table_names = [table_names]
    if persist_snapshots:
        logger.warning(
            '`persist_snapshots` is deprecated, use `initial_snapshot="table"` instead.'
        )
        if initial_snapshot is None:
            initial_snapshot = "table"
    if initial_snapshot == "direct":
        ensure_sqlalchemy_2()
    if backend == "pyarrow":
        ensure_pyarrow_backend()

    rep_conn = _get_rep_conn(credentials)
    keep_rep_conn = False
    try:
        with rep_conn.cursor() as cur:
            owned = get_publication_owned(pub_name, cur)
            if reset:
                drop_replication_slot(slot_name, cur)
                if owned:
                    drop_publication(pub_name, cur)
                    owned = None
                elif owned is False:
                    logger.warning(
                        f"Publication {pub_name} is kept: it belongs to another role."
                    )
            if owned is None:
                create_publication(pub_name, cur, publish, schema_name, table_names)
            pub_ops = get_pub_ops(pub_name, cur)
            if pub_ops["update"] or pub_ops["delete"]:
                # checked before the tables are published, Postgres would reject their updates
                tables_without_key = get_tables_without_key(
                    schema_name, table_names, cur
                )
                if tables_without_key:
                    raise NoPrimaryKeyException(schema_name, tables_without_key)
            if table_names is None:
                if not is_schema_published(pub_name, schema_name, cur):
                    add_schema_to_publication(schema_name, pub_name, cur)
            else:
                published = get_published_tables(pub_name, schema_name, cur)
                missing = [name for name in table_names if name not in published]
                if missing:
                    add_tables_to_publication(missing, schema_name, pub_name, cur)
            if pub_ops["update"]:
                warn_unchanged_toast(pub_name, cur, include_columns, exclude_columns)
            published_columns = get_published_columns(pub_name, cur)
            check_published_names(
                pub_name, published_columns, include_columns, exclude_columns
            )
            snapshot_exclude = _without_unreplicated_columns(
                schema_name, published_columns, exclude_columns
            )

            # no commands may run on `rep_conn` after the slot is created, they would end the
            # exported snapshot
            slot = create_replication_slot(slot_name, cur)
        if initial_snapshot is None:
            return None
        write_disposition: TWriteDisposition = (
            "merge" if pub_ops["update"] or pub_ops["delete"] else "append"
        )
        snapshot_name: Optional[str] = None
        if slot is not None:
            snapshot_name = slot["snapshot_name"]
        elif write_disposition == "append":
            raise ValueError(
                f"Slot {slot_name} already exists, the initial load overlaps with replicated"
                " changes. Only a publication of updates or deletes merges the overlap, publication"
                f" {pub_name} publishes inserts and the rows would be loaded twice."
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

        if initial_snapshot == "direct":
            snapshot = ReplicationSnapshot(rep_conn, snapshot_name, reflect_not_null)
            keep_rep_conn = True
            for table_name in table_names:
                resource = core_sql_table(
                    credentials=credentials,
                    table=table_name,
                    schema=schema_name,
                    reflection_level=reflection_level,
                    backend="pyarrow" if backend == "pyarrow" else "sqlalchemy",
                    included_columns=_columns_of_table(include_columns, table_name),
                    excluded_columns=_columns_of_table(snapshot_exclude, table_name),
                    type_adapter_callback=snapshot.type_adapter_callback,
                    engine_adapter_callback=snapshot.engine_adapter_callback,
                    table_adapter_callback=snapshot.table_adapter_callback,
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
                cur_snap.execute("START TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
                if snapshot_name:
                    cur_snap.execute("SET TRANSACTION SNAPSHOT %s;", (snapshot_name,))
                copy_suffix = snapshot_name or uniq_id(8)
                snapshot_tables = [
                    (
                        table_name,
                        persist_snapshot_table(
                            snapshot_name=copy_suffix,
                            table_name=table_name,
                            schema_name=schema_name,
                            cur=cur_snap,
                            include_columns=_columns_of_table(
                                include_columns, table_name
                            ),
                            exclude_columns=_columns_of_table(
                                snapshot_exclude, table_name
                            ),
                            unlogged=initial_snapshot == "unlogged_table",
                        ),
                        _get_pk(cur_snap, table_name, schema_name),
                        (
                            _get_not_null_columns(cur_snap, table_name, schema_name)
                            if reflect_not_null
                            else []
                        ),
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
                columns=_with_not_null(
                    None if columns is None else columns.get(table_name), not_null
                ),
                primary_key=primary_key,
                credentials=credentials,
                backend=backend,
                reflection_level=reflection_level,
            )
            for table_name, snapshot_table_name, primary_key, not_null in snapshot_tables
        ]
        if len(snapshot_table_resources) == 1:
            return snapshot_table_resources[0]
        return snapshot_table_resources
    finally:
        if not keep_rep_conn:
            rep_conn.close()


@with_config(
    sections=("sources", "pg_replication"),
    sections_merge_style=ConfigSectionContext.resource_merge_style,
    section_arg_name="slot_name",
)
def replication_slot_exists(
    slot_name: str = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
) -> bool:
    """Tells if replication slot `slot_name` exists.

    The slot exports a snapshot only when `init_replication` creates it. Check the slot before
    `init_replication` to pass `initial_snapshot` for all tables only on the first run.
    """
    with closing(_get_conn(credentials)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s;", (slot_name,)
            )
            return cur.fetchone() is not None


def ensure_pyarrow_backend() -> None:
    """Raises when `pyarrow` or `numpy` of the `pyarrow` backend is not installed"""
    try:
        import numpy  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError as ex:
        raise MissingDependencyException(
            "pg_replication pyarrow backend", ["pyarrow", "numpy"]
        ) from ex


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
    schema_name: Optional[str] = None,
    table_names: Optional[Sequence[str]] = None,
) -> None:
    """Creates a publication for logical replication if it doesn't exist yet.

    Does nothing if the publication already exists. Raises `PublicationPrivilegesException`
    with the statement that creates it with `table_names` or the whole schema if the user does
    not have the CREATE privilege for the database.
    """
    esc_name = escape_postgres_identifier(name)
    try:
        cur.execute(f"CREATE PUBLICATION {esc_name} WITH (publish = %s);", (publish,))
        logger.info(
            f"Successfully created publication {esc_name} with publish = '{publish}'."
        )
    except psycopg2.errors.DuplicateObject:  # the publication already exists
        logger.info(f'Publication "{name}" already exists.')
    except psycopg2.errors.InsufficientPrivilege:
        if table_names is not None:
            members = "TABLE " + ", ".join(
                _make_qualified_table_name(table_name, schema_name)
                for table_name in table_names
            )
        else:
            members = f"TABLES IN SCHEMA {escape_postgres_identifier(schema_name)}"
        raise PublicationPrivilegesException(
            name,
            f"CREATE PUBLICATION {esc_name} FOR {members} WITH (publish = '{publish}');",
        )


def get_publication_owned(name: str, cur: cursor) -> Optional[bool]:
    """Tells if the user may alter and drop the publication, None if it does not exist."""
    cur.execute(
        "SELECT pg_has_role(pubowner, 'USAGE') FROM pg_publication WHERE pubname = %s;",
        (name,),
    )
    row = cur.fetchone()
    return None if row is None else bool(row[0])


def get_published_tables(pub_name: str, schema_name: str, cur: cursor) -> List[str]:
    """Returns the names of tables of `schema_name` that the publication publishes."""
    cur.execute(
        "SELECT tablename FROM pg_publication_tables WHERE pubname = %s AND schemaname = %s;",
        (pub_name, schema_name),
    )
    return [row[0] for row in cur.fetchall()]


def get_published_columns(
    pub_name: str, cur: cursor
) -> Dict[Tuple[str, str], List[Tuple[str, bool]]]:
    """Returns the columns of published tables, with True if Postgres replicates their values.

    Generated columns are replicated only by Postgres 18 with `publish_generated_columns`.
    """
    generated = "a.attgenerated" if get_pg_version(cur) >= 120000 else "''::\"char\""
    cur.execute(
        f"""
        SELECT pt.schemaname, pt.tablename, a.attname, {generated} = ''
          OR ({generated} = 's' AND to_jsonb(p) ->> 'pubgencols' IN ('s', 'true'))
        FROM pg_publication_tables pt
        JOIN pg_publication p ON p.pubname = pt.pubname
        JOIN pg_namespace n ON n.nspname = pt.schemaname
        JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        WHERE pt.pubname = %s
        ORDER BY pt.schemaname, pt.tablename, a.attnum;
        """,
        (pub_name,),
    )
    columns: Dict[Tuple[str, str], List[Tuple[str, bool]]] = {}
    for schema_name, table_name, column_name, replicated in cur.fetchall():
        columns.setdefault((schema_name, table_name), []).append(
            (column_name, replicated)
        )
    return columns


def check_table_names(
    pub_name: str,
    tables: Iterable[Tuple[str, str]],
    normalize: Callable[[str], str],
) -> None:
    """Raises if published tables would load into the same destination table."""
    published = {
        f"{schema_name}.{table_name}": table_name for schema_name, table_name in tables
    }
    if collisions := name_collisions(
        list(published), lambda qual_name: normalize(published[qual_name])
    ):
        raise TableNameCollisionException(pub_name, collisions[0])


def is_schema_published(pub_name: str, schema_name: str, cur: cursor) -> bool:
    """Tells if the publication publishes all tables, or all tables of `schema_name`."""
    cur.execute(
        "SELECT puballtables FROM pg_publication WHERE pubname = %s;", (pub_name,)
    )
    row = cur.fetchone()
    if row is not None and row[0]:
        return True
    if get_pg_version(cur) < 150000:
        return False
    cur.execute(
        "SELECT 1 FROM pg_publication_namespace pn"
        " JOIN pg_publication p ON p.oid = pn.pnpubid"
        " JOIN pg_namespace n ON n.oid = pn.pnnspid"
        " WHERE p.pubname = %s AND n.nspname = %s;",
        (pub_name, schema_name),
    )
    return cur.fetchone() is not None


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
    statement = f"ALTER PUBLICATION {esc_pub_name} ADD TABLE {qual_name};"
    try:
        cur.execute(statement)
        logger.info(
            f"Successfully added table {qual_name} to publication {esc_pub_name}."
        )
    except psycopg2.errors.DuplicateObject:
        logger.info(
            f"Table {qual_name} is already a member of publication {esc_pub_name}."
        )
    except psycopg2.errors.InsufficientPrivilege:
        raise PublicationPrivilegesException(pub_name, statement)


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
    statement = (
        f"ALTER PUBLICATION {esc_pub_name} ADD TABLES IN SCHEMA {esc_schema_name};"
    )
    try:
        cur.execute(statement)
        logger.info(
            f"Successfully added schema {esc_schema_name} to publication {esc_pub_name}."
        )
    except psycopg2.errors.DuplicateObject:
        logger.info(
            f"Schema {esc_schema_name} is already a member of publication {esc_pub_name}."
        )
    except psycopg2.errors.InsufficientPrivilege:
        raise PublicationPrivilegesException(pub_name, statement)


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
    reflection_level: TReflectionLevel = "full",
) -> DltResource:
    """Returns a resource for a persisted snapshot table.

    Can be used to perform an initial load of the table, so all data that
    existed in the table prior to initializing replication is also captured.
    """
    resource: DltResource = core_sql_table(
        credentials=credentials,
        table=snapshot_table_name,
        schema=schema_name,
        reflection_level=reflection_level,
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


def check_published_names(
    pub_name: str,
    published_columns: Mapping[Tuple[str, str], Sequence[Tuple[str, bool]]],
    include_columns: Optional[Mapping[str, Sequence[str]]] = None,
    exclude_columns: Optional[Mapping[str, Sequence[str]]] = None,
) -> None:
    """Raises if published tables or their columns would load into the same destination one."""
    naming = Schema("pg_replication").naming
    check_table_names(pub_name, published_columns, naming.normalize_table_identifier)
    for (schema_name, table_name), columns in published_columns.items():
        include = _columns_of_table(include_columns, table_name)
        exclude = _columns_of_table(exclude_columns, table_name)
        names = [
            name
            for name, replicated in columns
            if replicated and _is_column_included(name, include, exclude)
        ]
        if collisions := name_collisions(names, naming.normalize_identifier):
            raise ColumnNameCollisionException(schema_name, table_name, collisions[0])


def _without_unreplicated_columns(
    schema_name: str,
    published_columns: Mapping[Tuple[str, str], Sequence[Tuple[str, bool]]],
    exclude_columns: Optional[Mapping[str, Sequence[str]]] = None,
) -> Dict[str, Sequence[str]]:
    """Adds columns whose values Postgres does not replicate to `exclude_columns`.

    A snapshot with them would have values that replicated changes set to NULL.
    """
    excluded = dict(exclude_columns or {})
    for (table_schema, table_name), columns in published_columns.items():
        unreplicated = [name for name, replicated in columns if not replicated]
        if table_schema != schema_name or not unreplicated:
            continue
        logger.warning(
            f"Generated columns {', '.join(unreplicated)} of table {schema_name}.{table_name}"
            " are left out of the initial snapshot because Postgres does not replicate their"
            " values. Postgres 18 replicates stored generated columns of publications with"
            " `publish_generated_columns = stored`."
        )
        excluded[table_name] = [*excluded.get(table_name, []), *unreplicated]
    return excluded


def name_collisions(
    names: Sequence[str], normalize: Callable[[str], str]
) -> List[List[str]]:
    """Returns groups of `names` that normalize to the same identifier."""
    groups: Dict[str, List[str]] = {}
    for name in names:
        groups.setdefault(normalize(name), []).append(name)
    return [group for group in groups.values() if len(group) > 1]


def _is_column_included(
    column_name: str,
    include_columns: Optional[Sequence[str]],
    exclude_columns: Optional[Sequence[str]],
) -> bool:
    return (include_columns is None or column_name in include_columns) and (
        exclude_columns is None or column_name not in exclude_columns
    )


def _with_not_null(
    columns: Optional[TTableSchemaColumns], not_null: Sequence[str]
) -> Optional[TTableSchemaColumns]:
    """Adds NOT NULL to `columns` hints, snapshot copies have no NOT NULL columns."""
    if not not_null:
        return columns
    merged: TTableSchemaColumns = {
        name: {"name": name, "nullable": False} for name in not_null
    }
    for name, hint in (columns or {}).items():
        merged[name] = {**merged.get(name, {}), **hint}
    return merged


def _get_not_null_columns(cur: cursor, table_name: str, schema_name: str) -> List[str]:
    """Returns the names of NOT NULL columns of a Postgres table."""
    cur.execute(
        "SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass AND attnotnull"
        " AND attnum > 0 AND NOT attisdropped;",
        (_make_qualified_table_name(table_name, schema_name),),
    )
    return [row[0] for row in cur.fetchall()]


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

    def get_not_null_columns(self, relation_id: int) -> List[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid = %s AND attnotnull"
                " AND attnum > 0 AND NOT attisdropped;",
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
    not_null_columns: List[str] = field(default_factory=list)


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
    insert_final_lsns: List[int] = field(default_factory=list)
    row_final_lsns: List[int] = field(default_factory=list)

    def position(self, slot_name: str, index: Optional[int] = None) -> ChangePosition:
        """Returns the transaction of the row at `index`, inserts first, or of all rows."""
        final_lsns = self.insert_final_lsns + self.row_final_lsns
        if index is None:
            return ChangePosition(slot_name, min(final_lsns), max(final_lsns))
        return ChangePosition(slot_name, final_lsns[index], final_lsns[index])


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
        reflection_level: TReflectionLevel = "full",
        destination_schema: Optional[Schema] = None,
        decimal_precision: Tuple[int, int] = (
            DEFAULT_NUMERIC_PRECISION,
            DEFAULT_NUMERIC_SCALE,
        ),
        reflect_not_null: bool = False,
        resolve_not_null: Optional[Callable[[int], List[str]]] = None,
        hard_delete: bool = True,
        slot_name: str = "",
    ) -> None:
        self.pub_ops = pub_ops
        self.upto_lsn = upto_lsn
        self.target_batch_size = target_batch_size
        self.include_columns = include_columns
        self.exclude_columns = exclude_columns
        self.columns = columns
        self.resolve_type = resolve_type
        self.resolve_primary_key = resolve_primary_key
        self.reflection_level = reflection_level
        self.destination_schema = destination_schema
        self.decimal_precision = decimal_precision
        self.reflect_not_null = reflect_not_null
        self.resolve_not_null = resolve_not_null
        self.hard_delete = hard_delete
        self.slot_name = slot_name
        self.naming = (destination_schema or Schema("pg_replication")).naming

        self.relations: Dict[int, RelationSchema] = {}
        self._emitted_columns: Dict[str, TTableSchemaColumns] = {}
        self.segments: List[Segment] = []
        self.open_segments: Dict[int, Segment] = {}
        self.n_items = 0
        self.in_transaction = False
        self.commit_ts: Optional[datetime] = None
        self.final_lsn = 0
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
                segment.insert_final_lsns.append(self.final_lsn)
                self.n_items += 1
        elif op in (85, 68):  # U, D
            self.process_change(payload, data_start)
        elif op == 66:  # B
            self.in_transaction = True
            begin = decode_begin(payload)
            self.commit_ts = begin.commit_ts
            self.final_lsn = begin.final_lsn
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
            raise ValueError(f"Unknown replication op {op} in {self._position()}")

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
            raise NoPrimaryKeyException(
                relation.namespace, [table_name], self._position()
            )
        include = _columns_of_table(self.include_columns, table_name)
        exclude = _columns_of_table(self.exclude_columns, table_name)
        included_indices = [
            i
            for i, c in enumerate(relation.columns)
            if _is_column_included(c.name, include, exclude)
        ]
        if not skip and (
            collisions := name_collisions(
                [relation.columns[i].name for i in included_indices],
                self.naming.normalize_identifier,
            )
        ):
            raise ColumnNameCollisionException(
                relation.namespace, table_name, collisions[0], self._position()
            )

        hints = {} if self.columns is None else self.columns.get(table_name, {})
        column_schemas: TTableSchemaColumns = {}
        for i in included_indices:
            column = relation.columns[i]
            column_schemas[column.name] = to_dlt_column_schema(
                column, pg_types[i], i in key_indices, self.reflection_level
            )
            # a type in `columns` wins, the user takes care of the destination column
            if not skip and not set(COLUMN_TYPE_HINTS) & set(
                hints.get(column.name, {})
            ):
                self._keep_destination_type(
                    relation, column_schemas[column.name], pg_types[i]
                )
        # only a new table gets NOT NULL, a NOT NULL column can't be added to a table with rows
        if (
            self.reflect_not_null
            and self.resolve_not_null
            and not skip
            and table_name not in self._emitted_columns
            and self._existing_table(table_name) is None
        ):
            for name in self.resolve_not_null(relation.relation_id):
                if name in column_schemas:
                    column_schemas[name]["nullable"] = False
        for column_name, column_hint in hints.items():
            if column_name in column_schemas:
                column_schemas[column_name] = merge_column(
                    column_schemas[column_name], column_hint
                )
            else:
                column_schemas[column_name] = {"name": column_name, **column_hint}

        # add hints for replication columns
        column_schemas[LSN] = {"name": LSN, "data_type": "bigint", "nullable": True}
        if self.pub_ops["update"] or self.pub_ops["delete"]:
            column_schemas[LSN]["dedup_sort"] = "desc"
        if self.pub_ops["delete"]:
            column_schemas[DELETED_TS] = {
                "name": DELETED_TS,
                "data_type": "timestamp",
                "nullable": True,
            }
            if self.hard_delete:
                column_schemas[DELETED_TS]["hard_delete"] = True
        # dlt takes the first `dedup_sort` and `hard_delete` column, clear them on old columns
        legacy = self._existing_column(table_name, LEGACY_LSN)
        if legacy and legacy.get("dedup_sort"):
            column_schemas.setdefault(LEGACY_LSN, {"name": LEGACY_LSN})[
                "dedup_sort"
            ] = None
        legacy = self._existing_column(table_name, LEGACY_DELETED_TS)
        if legacy and legacy.get("hard_delete"):
            column_schemas.setdefault(LEGACY_DELETED_TS, {"name": LEGACY_DELETED_TS})[
                "hard_delete"
            ] = False
        write_disposition: TWriteDisposition = "append"
        if self.pub_ops["update"] or self.pub_ops["delete"]:
            write_disposition = "merge"

        not_null_columns: List[str] = []
        if not skip:
            missing = self._missing_not_null_columns(table_name, column_schemas)
            if missing:
                raise NullInNotNullColumnException(
                    relation.namespace,
                    table_name,
                    missing,
                    self._position(),
                    missing=True,
                )
            not_null_columns = [
                relation.columns[i].name
                for i in included_indices
                if i not in key_indices
                and column_schemas[relation.columns[i].name].get("nullable") is False
            ]

        self._emitted_columns[table_name] = column_schemas
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
            not_null_columns=not_null_columns,
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
                    relation.schema_name, relation.table_name, missing, self._position()
                )
        self._add_row(segment, new_values, data_start, None)

    def _keep_destination_type(
        self, relation: Relation, column_schema: TColumnSchema, pg_type: PgType
    ) -> None:
        """Replaces the inferred type with the type of the column loaded before, if any.

        Raises `IncompatibleColumnTypeException` if a value of `pg_type` may not fit it.
        """
        existing = self._existing_column(relation.relation_name, column_schema["name"])
        if not existing or "data_type" not in existing:
            return
        if not column_type_fits(existing, pg_type, self.decimal_precision):
            raise IncompatibleColumnTypeException(
                relation.namespace,
                relation.relation_name,
                column_schema["name"],
                format_column_type(existing),
                format_column_type(to_dlt_column_type(pg_type, "full_with_precision")),
                dict(to_dlt_column_type(pg_type, self.reflection_level)),
                self._position(),
            )
        schema = cast(Dict[str, Any], column_schema)
        for hint in (*COLUMN_TYPE_HINTS, "nullable"):
            schema.pop(hint, None)
            if hint in existing:
                schema[hint] = existing[hint]  # type: ignore[literal-required]

    def _existing_column(
        self, table_name: str, column_name: str
    ) -> Optional[TColumnSchema]:
        """Returns the column emitted earlier in this run or in `destination_schema`."""
        if table_name in self._emitted_columns:
            return self._emitted_columns[table_name].get(column_name)
        if existing := self._existing_table(table_name):
            schema, columns = existing
            return columns.get(schema.naming.normalize_identifier(column_name))
        return None

    def _existing_table(
        self, table_name: str
    ) -> Optional[Tuple[Schema, TTableSchemaColumns]]:
        """Returns the columns of `table_name` in `destination_schema`, None if not loaded."""
        schema = self.destination_schema
        if schema is None:
            return None
        table = schema.tables.get(schema.naming.normalize_table_identifier(table_name))
        return None if table is None else (schema, table["columns"])

    def _missing_not_null_columns(
        self, table_name: str, column_schemas: TTableSchemaColumns
    ) -> List[str]:
        """Returns NOT NULL columns of the destination table that get no values."""
        existing = self._existing_table(table_name)
        if existing is None:
            return []
        schema, columns = existing
        emitted = {schema.naming.normalize_identifier(name) for name in column_schemas}
        return [
            name
            for name, column in columns.items()
            if column.get("nullable") is False
            and name not in emitted
            and not name.startswith(DLT_NAME_PREFIX)
        ]

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
        segment.row_final_lsns.append(self.final_lsn)
        self.n_items += 1

    def _position(self) -> ChangePosition:
        return ChangePosition(self.slot_name, self.final_lsn, self.final_lsn)


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


def segment_to_rows(segment: Segment, slot_name: str = "") -> List[TDataItem]:
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
    with_deletes = DELETED_TS in relation.column_schemas
    hard_delete = with_deletes and bool(
        relation.column_schemas[DELETED_TS].get("hard_delete")
    )
    rows: List[TDataItem] = []

    def _to_row(values: TupleValues) -> Dict[str, Any]:
        selected = [values[i] for i in indices]
        try:
            return {
                name: value if value is None or convert is None else convert(value)
                for name, convert, value in zip(names, converters, selected)
            }
        except Exception as ex:
            raise ValueConversionException(
                relation.schema_name,
                relation.table_name,
                _failing_column(names, converters, selected),
                str(ex),
                segment.position(slot_name, len(rows)),
            ) from ex

    for payload, lsn in zip(segment.insert_payloads, segment.insert_lsns):
        row = _to_row(read_tuple(payload, 6)[0])
        row[LSN] = lsn
        rows.append(row)
    for values, lsn, deleted_ts in zip(
        segment.rows, segment.row_lsns, segment.row_deleted_ts
    ):
        row = _to_row(values)
        row[LSN] = lsn
        if deleted_ts is not None and hard_delete:
            # replace NULLs to prevent NOT NULL violations in staging table
            for name, dummy in zip(names, dummies):
                if row[name] is None:
                    row[name] = dummy
        if with_deletes:
            row[DELETED_TS] = deleted_ts
        rows.append(row)
    for index, row in enumerate(rows if relation.not_null_columns else []):
        if nulls := [name for name in relation.not_null_columns if row[name] is None]:
            raise NullInNotNullColumnException(
                relation.schema_name,
                relation.table_name,
                nulls,
                segment.position(slot_name, index),
            )
    return rows


def _failing_column(
    names: Sequence[str],
    converters: Sequence[Optional[TValueConverter]],
    values: Sequence[Any],
) -> Optional[str]:
    for name, convert, value in zip(names, converters, values):
        if value is not None and convert is not None:
            try:
                convert(value)
            except Exception:
                return name
    return None


def get_destination_schema(
    snapshot_pipeline: Optional[dlt.Pipeline], first_run: bool
) -> Optional[Schema]:
    """Returns the schema of the tables loaded before, None outside of a pipeline run.

    The first replication run takes the schema of `snapshot_pipeline` if given. Later runs take
    the schema of the running pipeline, which has the replicated tables by then.
    """
    if first_run and snapshot_pipeline is not None:
        if not snapshot_pipeline.default_schema_name:
            raise SnapshotSchemaNotFoundException(snapshot_pipeline.pipeline_name)
        return snapshot_pipeline.default_schema
    container = Container()
    if (
        not container[PipelineContext].is_active()
        or SourceSchemaInjectableContext not in container
    ):
        return None
    pipeline = dlt.current.pipeline()
    # the source schema of a resource is a copy without data tables
    schema_name = dlt.current.source_schema().name
    return pipeline.schemas[schema_name] if schema_name in pipeline.schemas else None


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
    reflection_level: TReflectionLevel = "full",
    reflect_not_null: bool = False,
    destination_schema: Optional[Schema] = None,
    hard_delete: bool = True,
) -> Iterator[ChangesBatch]:
    """Streams changes from the slot until the WAL position flushed at the start is reached.

    Uses a single replication connection. With `flush_slot`, the slot is confirmed up to
    `start_lsn` at the start, i.e. changes of the previous run are discarded only when a
    pipeline run with the state containing `start_lsn` succeeded. The last batch has the
    position up to which all changes were received, also when none were published.
    Columns loaded before keep their types, found in `destination_schema`.
    """
    caps = (
        Container().get(DestinationCapabilitiesContext)
        or DestinationCapabilitiesContext.generic_capabilities()
    )
    if backend == "pyarrow":
        ensure_pyarrow_backend()
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
            reflection_level=reflection_level,
            destination_schema=destination_schema,
            decimal_precision=caps.decimal_precision
            or (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE),
            reflect_not_null=reflect_not_null,
            resolve_not_null=catalog.get_not_null_columns,
            hard_delete=hard_delete,
            slot_name=slot_name,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = %s;",
                (pub_name,),
            )
            check_table_names(
                pub_name, cur.fetchall(), consumer.naming.normalize_table_identifier
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
                        segment_to_data(segment, slot_name), segment.relation.table_name
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
