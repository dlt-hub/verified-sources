from typing import (
    Optional,
    Dict,
    Mapping,
    Iterator,
    Union,
    List,
    Sequence,
    Any,
)
from dataclasses import dataclass, field

import psycopg2
from psycopg2.extensions import cursor, connection as ConnectionExt
from psycopg2.extras import (
    LogicalReplicationConnection,
    ReplicationCursor,
    ReplicationMessage,
    StopReplication,
)

import dlt
from dlt.common import logger
from dlt.common.typing import TDataItem
from dlt.common.pendulum import pendulum
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.common.schema.typing import (
    TTableSchema,
    TTableSchemaColumns,
    TColumnNames,
    TWriteDisposition,
    DLT_NAME_PREFIX,
)
from dlt.common.schema.utils import merge_column
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.data_writers.escape import escape_postgres_identifier

from dlt.extract.items import DataItemWithMeta
from dlt.extract import DltResource

from dlt.sources.config import with_config
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import (
    sql_table as core_sql_table,
    sql_database as core_sql_database,
)

from .schema_types import _to_dlt_column_schema, _to_dlt_val
from .exceptions import IncompatiblePostgresVersionException
from .decoders import (
    Begin,
    Relation,
    Insert,
    Update,
    Delete,
    ColumnData,
)

__source_name__ = "pg_replication"


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
    columns: Optional[Mapping[str, TTableSchemaColumns]] = None,
    reset: bool = False,
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
    - table snapshots can only be persisted on the first call (because the snapshot
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
        persist_snapshots (bool): Whether the table states in the snapshot exported
          during replication slot creation are persisted to tables. If true, a
          snapshot table is created in Postgres for all included tables, and corresponding
          resources (`DltResource` objects) for these tables are created and returned.
          The resources can be used to perform an initial load of all data present
          in the tables at the moment the replication slot got created.
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
          Argument is only used if `persist_snapshots` is `True`.
        columns (Optional[Dict[str, TTableSchemaColumns]]): Maps
          table name(s) to column hints to apply on the snapshot table resource(s).
          For example:
          ```
          columns={
              "table_x": {"col_a": {"data_type": "json"}},
              "table_y": {"col_y": {"precision": 32}},
          }
          ```
          Argument is only used if `persist_snapshots` is `True`.
        reset (bool): If set to True, the existing slot and publication are dropped
          and recreated. Has no effect if a slot and publication with the provided
          names do not yet exist.

    Returns:
        - None if `persist_snapshots` is `False`
        - a `DltResource` object or a list of `DltResource` objects for the snapshot
          table(s) if `persist_snapshots` is `True` and the replication slot did not yet exist
    """
    if isinstance(table_names, str):
        table_names = [table_names]

    rep_conn = _get_rep_conn(credentials)
    try:
        with rep_conn.cursor() as cur:
            if reset:
                drop_replication_slot(slot_name, cur)
                drop_publication(pub_name, cur)
            create_publication(pub_name, cur, publish)
            if table_names is None:
                add_schema_to_publication(schema_name, pub_name, cur)
            else:
                add_tables_to_publication(table_names, schema_name, pub_name, cur)

            slot = create_replication_slot(slot_name, cur)
            if persist_snapshots:
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

                # need separate session to read the snapshot: https://stackoverflow.com/q/75852587
                with _get_conn(credentials) as conn:
                    with conn.cursor() as cur_snap:
                        # set the snapshot to the snaphost of the newly created replication slot
                        cur_snap.execute(
                            f"""
                            START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
                            SET TRANSACTION SNAPSHOT '{slot["snapshot_name"]}';
                        """
                        )
                        snapshot_tables = [
                            (
                                table_name,
                                persist_snapshot_table(
                                    snapshot_name=slot["snapshot_name"],
                                    table_name=table_name,
                                    schema_name=schema_name,
                                    cur=cur_snap,
                                    include_columns=(
                                        None
                                        if include_columns is None
                                        else include_columns.get(table_name)
                                    ),
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
                        write_disposition="append" if publish == "insert" else "merge",
                        columns=None if columns is None else columns.get(table_name),
                        primary_key=primary_key,
                        credentials=credentials,
                    )
                    for table_name, snapshot_table_name, primary_key in snapshot_tables
                ]
                if len(snapshot_table_resources) == 1:
                    return snapshot_table_resources[0]
                return snapshot_table_resources
    except Exception:
        rep_conn.rollback()
        raise
    else:
        rep_conn.commit()
    finally:
        rep_conn.close()
    return None


@dlt.sources.config.with_config(sections=("sources", "pg_replication"))
def get_pg_version(
    cur: cursor = None,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
) -> int:
    """Returns Postgres server version as int."""
    if cur is not None:
        return cur.connection.server_version
    with _get_conn(credentials) as conn:
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
        cur.execute(f"CREATE PUBLICATION {esc_name} WITH (publish = '{publish}');")
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
        table_names = table_names
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
            f"Cannot add schema to publication because the Postgres server version {version} is too low."
            " Adding schemas to a publication is only supported for Postgres version 15 or higher."
            " Upgrade your Postgres server version or set the `table_names` argument to explicitly specify table names."
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
) -> str:
    """Persists exported snapshot table state.

    Reads snapshot table content and copies it into new table.
    """
    col_str = "*"
    if include_columns is not None:
        col_str = ", ".join(map(escape_postgres_identifier, include_columns))
    # make sure to shorten identifier
    naming = NamingConvention(63)
    # name must start with _dlt so we skip this table when replicating
    snapshot_table_name = naming.normalize_table_identifier(
        f"_dlt_{table_name}_s_{snapshot_name}"
    )
    snapshot_qual_name = _make_qualified_table_name(snapshot_table_name, schema_name)
    qual_name = _make_qualified_table_name(table_name, schema_name)
    cur.execute(
        f"""
        CREATE TABLE {snapshot_qual_name} AS SELECT {col_str} FROM {qual_name};
        """
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
    )
    resource.apply_hints(
        table_name=table_name,
        write_disposition=write_disposition,
        columns=columns,
        primary_key=primary_key,
    )
    return resource


def get_max_lsn(
    slot_name: str,
    options: Dict[str, str],
    credentials: ConnectionStringCredentials,
) -> Optional[int]:
    """Returns maximum Log Sequence Number (LSN) in replication slot.

    Returns None if the replication slot is empty.
    Does not consume the slot, i.e. messages are not flushed.
    Raises error if the replication slot or publication does not exist.
    """
    # comma-separated value string
    options_str = ", ".join(
        f"'{x}'" for xs in list(map(list, options.items())) for x in xs
    )
    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(lsn) - '0/0' AS max_lsn "  # subtract '0/0' to convert pg_lsn type to int (https://stackoverflow.com/a/73738472)
                f"FROM pg_logical_slot_peek_binary_changes('{slot_name}', NULL, NULL, {options_str});"
            )
            lsn: int = cur.fetchone()[0]
        return lsn


def get_pub_ops(
    pub_name: str,
    cur: cursor,
) -> Dict[str, bool]:
    """Returns dictionary of DML operations and their publish status."""

    cur.execute(
        f"""
        SELECT pubinsert, pubupdate, pubdelete, pubtruncate
        FROM pg_publication WHERE pubname = '{pub_name}'
    """
    )
    result = cur.fetchone()
    # credentials.connection.commit()
    if result is None:
        raise ValueError(f'Publication "{pub_name}" does not exist.')
    return {
        "insert": result[0],
        "update": result[1],
        "delete": result[2],
        "truncate": result[3],
    }


def lsn_int_to_hex(lsn: int) -> str:
    """Convert integer LSN to postgres' hexadecimal representation."""
    # https://stackoverflow.com/questions/66797767/lsn-external-representation.
    return f"{lsn >> 32 & 4294967295:X}/{lsn & 4294967295:08X}"


def advance_slot(
    upto_lsn: int,
    slot_name: str,
    credentials: ConnectionStringCredentials,
) -> None:
    """Advances position in the replication slot.

    Flushes all messages upto (and including) the message with LSN = `upto_lsn`.
    This function is used as alternative to psycopg2's `send_feedback` method, because
    the behavior of that method seems odd when used outside of `consume_stream`.
    """
    if upto_lsn != 0:
        with _get_conn(credentials) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM pg_replication_slot_advance('{slot_name}', '{lsn_int_to_hex(upto_lsn)}');"
                )


def _get_conn(
    credentials: ConnectionStringCredentials,
    connection_factory: Optional[Any] = None,
) -> ConnectionExt:
    """Returns a psycopg2 connection to interact with postgres."""
    conn = psycopg2.connect(
        dsn=credentials.to_native_representation(),
        connection_factory=connection_factory,
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
        f"""
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{qual_name}'::regclass
        AND    i.indisprimary;
    """
    )
    result = [tup[0] for tup in cur.fetchall()]
    if len(result) == 0:
        return None
    elif len(result) == 1:
        return result[0]  # type: ignore[no-any-return]
    return result


@dataclass
class ItemGenerator:
    credentials: ConnectionStringCredentials
    slot_name: str
    options: Dict[str, str]
    upto_lsn: int
    start_lsn: int = 0
    target_batch_size: int = 1000
    include_columns: Optional[Dict[str, Sequence[str]]] = (None,)  # type: ignore[assignment]
    columns: Optional[Dict[str, TTableSchemaColumns]] = (None,)  # type: ignore[assignment]
    last_commit_lsn: Optional[int] = field(default=None, init=False)
    generated_all: bool = False

    def __iter__(self) -> Iterator[Union[TDataItem, DataItemWithMeta]]:
        """Yields replication messages from MessageConsumer.

        Starts replication of messages published by the publication from the replication slot.
        Maintains LSN of last consumed Commit message in object state.
        Does not advance the slot.
        """
        try:
            cur = _get_rep_conn(self.credentials).cursor()
            pub_ops = get_pub_ops(self.options["publication_names"], cur)
            cur.start_replication(
                slot_name=self.slot_name,
                start_lsn=self.start_lsn,
                decode=False,
                options=self.options,
            )
            consumer = MessageConsumer(
                upto_lsn=self.upto_lsn,
                pub_ops=pub_ops,
                target_batch_size=self.target_batch_size,
                include_columns=self.include_columns,
                columns=self.columns,
            )
            cur.consume_stream(consumer)
        except StopReplication:  # completed batch or reached `upto_lsn`
            pass
        finally:
            cur.connection.close()
        self.last_commit_lsn = consumer.last_commit_lsn
        for rel_id, data_items in consumer.data_items.items():
            table_name = consumer.last_table_schema[rel_id]["name"]
            # skip dlt internal tables
            # TODO: normalize the prefix. low prio. on postgres we should have lower case
            if table_name.lower().startswith(DLT_NAME_PREFIX):
                continue
            yield data_items[0]  # meta item with column hints only, no data
            yield dlt.mark.with_table_name(data_items[1:], table_name)
        self.generated_all = consumer.consumed_all


class MessageConsumer:
    """Consumes messages from a ReplicationCursor sequentially.

    Generates data item for each `insert`, `update`, and `delete` message.
    Processes in batches to limit memory usage.
    Maintains message data needed by subsequent messages in internal state.
    """

    def __init__(
        self,
        upto_lsn: int,
        pub_ops: Dict[str, bool],
        target_batch_size: int = 1000,
        include_columns: Optional[Dict[str, Sequence[str]]] = None,
        columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    ) -> None:
        self.upto_lsn = upto_lsn
        self.pub_ops = pub_ops
        self.target_batch_size = target_batch_size
        self.include_columns = include_columns
        self.columns = columns

        self.consumed_all: bool = False
        # data_items attribute maintains all data items
        self.data_items: Dict[
            int, List[Union[TDataItem, DataItemWithMeta]]
        ] = dict()  # maps relation_id to list of data items
        # other attributes only maintain last-seen values
        self.last_table_schema: Dict[
            int, TTableSchema
        ] = dict()  # maps relation_id to table schema
        self.last_commit_ts: pendulum.DateTime
        self.last_commit_lsn = None

    def __call__(self, msg: ReplicationMessage) -> None:
        """Processes message received from stream."""
        self.process_msg(msg)

    def process_msg(self, msg: ReplicationMessage) -> None:
        """Processes encoded replication message.

        Identifies message type and decodes accordingly.
        Message treatment is different for various message types.
        Breaks out of stream with StopReplication exception when
        - `upto_lsn` is reached
        - `target_batch_size` is reached
        - a table's schema has changed
        """
        op = msg.payload[0]
        if op == 73:  # ASCII for 'I'
            self.process_change(Insert(msg.payload), msg.data_start)
        elif op == 85:  # ASCII for 'U'
            self.process_change(Update(msg.payload), msg.data_start)
        elif op == 68:  # ASCII for 'D'
            self.process_change(Delete(msg.payload), msg.data_start)
        elif op == 66:  # ASCII for 'B'
            self.last_commit_ts = Begin(msg.payload).commit_ts  # type: ignore[assignment]
        elif op == 67:  # ASCII for 'C'
            self.process_commit(msg)
        elif op == 82:  # ASCII for 'R'
            self.process_relation(Relation(msg.payload))
        elif op == 84:  # ASCII for 'T'
            logger.warning(
                "The truncate operation is currently not supported. "
                "Truncate replication messages are ignored."
            )
        else:
            raise ValueError(f"Unknown replication op {op}")

    def process_commit(self, msg: ReplicationMessage) -> None:
        """Updates object state when Commit message is observed.

        Raises StopReplication when `upto_lsn` or `target_batch_size` is reached.
        """
        self.last_commit_lsn = msg.data_start
        if msg.data_start >= self.upto_lsn:
            self.consumed_all = True
        n_items = sum(
            [len(items) for items in self.data_items.values()]
        )  # combine items for all tables
        if self.consumed_all or n_items >= self.target_batch_size:
            raise StopReplication

    def process_relation(self, decoded_msg: Relation) -> None:
        """Processes a replication message of type Relation.

        Stores table schema in object state.
        Creates meta item to emit column hints while yielding data.

        Raises StopReplication when a table's schema changes.
        """
        if (
            self.data_items.get(decoded_msg.relation_id) is not None
        ):  # table schema change
            raise StopReplication
        # get table schema information from source and store in object state
        table_name = decoded_msg.relation_name
        columns: TTableSchemaColumns = {
            c.name: _to_dlt_column_schema(c) for c in decoded_msg.columns
        }

        self.last_table_schema[decoded_msg.relation_id] = {
            "name": table_name,
            "columns": columns,
        }

        # apply user input
        # 1) exclude columns
        include_columns = (
            None
            if self.include_columns is None
            else self.include_columns.get(table_name)
        )
        if include_columns is not None:
            columns = {k: v for k, v in columns.items() if k in include_columns}
        # 2) override source hints
        column_hints: TTableSchemaColumns = (
            dict() if self.columns is None else self.columns.get(table_name, dict())
        )
        for column_name, column_val in column_hints.items():
            columns[column_name] = merge_column(columns[column_name], column_val)

        # add hints for replication columns
        columns["lsn"] = {"data_type": "bigint", "nullable": True}
        if self.pub_ops["update"] or self.pub_ops["delete"]:
            columns["lsn"]["dedup_sort"] = "desc"
        if self.pub_ops["delete"]:
            columns["deleted_ts"] = {
                "hard_delete": True,
                "data_type": "timestamp",
                "nullable": True,
            }

        # determine write disposition
        write_disposition: TWriteDisposition = "append"
        if self.pub_ops["update"] or self.pub_ops["delete"]:
            write_disposition = "merge"

        # include meta item to emit hints while yielding data
        meta_item = dlt.mark.with_hints(
            [],
            dlt.mark.make_hints(
                table_name=table_name,
                write_disposition=write_disposition,
                columns=columns,
            ),
            create_table_variant=True,
        )
        self.data_items[decoded_msg.relation_id] = [meta_item]

    def process_change(
        self, decoded_msg: Union[Insert, Update, Delete], msg_start_lsn: int
    ) -> None:
        """Processes replication message of type Insert, Update, or Delete.

        Adds data item for inserted/updated/deleted record to instance attribute.
        """
        if isinstance(decoded_msg, (Insert, Update)):
            column_data = decoded_msg.new_tuple.column_data
        elif isinstance(decoded_msg, Delete):
            column_data = decoded_msg.old_tuple.column_data
        table_name = self.last_table_schema[decoded_msg.relation_id]["name"]
        data_item = self.gen_data_item(
            data=column_data,
            column_schema=self.last_table_schema[decoded_msg.relation_id]["columns"],
            lsn=msg_start_lsn,
            commit_ts=self.last_commit_ts,
            for_delete=isinstance(decoded_msg, Delete),
            include_columns=(
                None
                if self.include_columns is None
                else self.include_columns.get(table_name)
            ),
        )
        self.data_items[decoded_msg.relation_id].append(data_item)

    @staticmethod
    def gen_data_item(
        data: List[ColumnData],
        column_schema: TTableSchemaColumns,
        lsn: int,
        commit_ts: pendulum.DateTime,
        for_delete: bool,
        include_columns: Optional[Sequence[str]] = None,
    ) -> TDataItem:
        """Generates data item from replication message data and corresponding metadata."""
        data_item = {
            schema["name"]: _to_dlt_val(
                val=data.col_data,
                data_type=schema["data_type"],
                byte1=data.col_data_category,
                for_delete=for_delete,
            )
            for (schema, data) in zip(column_schema.values(), data)
            if (True if include_columns is None else schema["name"] in include_columns)
        }
        data_item["lsn"] = lsn
        if for_delete:
            data_item["deleted_ts"] = commit_ts
        return data_item
