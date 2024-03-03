from typing import Optional, Dict, Iterator, Union, List, Tuple, Sequence, Any
from dlt.common.pendulum import pendulum
from copy import deepcopy
from dataclasses import dataclass, field

import psycopg2
from psycopg2.extras import (
    LogicalReplicationConnection,
    ReplicationCursor,
    ReplicationMessage,
    StopReplication,
)
from pypgoutput.decoders import (  # type: ignore[import-untyped]
    Begin,
    Commit,
    Relation,
    Insert,
    Update,
    Delete,
    Truncate,
    ColumnData,
)

import dlt

from dlt.common import logger
from dlt.common.typing import TDataItem
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns
from dlt.common.data_writers.escape import escape_postgres_identifier
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.extract.typing import DataItemWithMeta
from dlt.sources import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from .. import sql_table

from .schema_types import _to_dlt_column_schema, _to_dlt_val


@configspec
class InitTableReplicationConfiguration(BaseConfiguration):
    credentials: ConnectionStringCredentials
    table: str
    schema: str
    include_columns: Optional[Sequence[str]]


@configspec
class TableChangesResourceConfiguration(BaseConfiguration):
    credentials: ConnectionStringCredentials
    table: str
    slot_name: str
    publication_name: str


@dlt.sources.config.with_config(
    sections=("sources", "sql_database"),
    spec=InitTableReplicationConfiguration,
)
def init_table_replication(
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    table: str = dlt.config.value,
    schema: str = dlt.config.value,
    publish: str = "insert, update, delete",
    persist_snapshot: bool = False,
    include_columns: Optional[Sequence[str]] = dlt.config.value,
    reset: bool = False,
) -> Tuple[str, str, Optional[DltResource]]:
    """Initializes replication for a table.

    Creates a replication slot and publication dedicated to the table if they do not exist yet.
    Does nothing if the slot and publication already exist and `reset` is set to `False`.

    Args:
        credentials (ConnectionStringCredentials): Postgres database credentials.
        table (str): Name of the table to initialize replication for.
        schema (str): Name of the schema the table belongs to.
        publish (str): Comma-separated string of DML operations. Can be used to
          control which changes are included in the publication. Allowed operations
          are `insert`, `update`, and `delete`. `truncate` is currently not
          supported—messages of that type are ignored.
          E.g. `publish="insert"` will create a publication that only publishes insert operations.
        persist_snapshot (bool): Whether the table state in the exported snapshot
          is persisted to a table. If true, a snapshot table is created in Postgres
          and a DltResource object for this table is returned. The DltResource
          can be used to perform an initial load of all data present in the table
          at the moment the replication slot got created.
        include_columns (Optional[Sequence[str]]): Optional sequence of names of
          columns to include in the snapshot table. Any columns not in the sequence
          are excluded. If not provided, all columns are included in the table.
        reset (bool): Whether replication for the table is reset. Only relevant
          if a replication slot and publication already exist for the table. If
          set to True, the existing slot and publication are dropped and recreated.

    Returns:
        Tuple with the names of the created slot and publication, and optionally
        a table snapshot resource.
    """
    slot_name, publication_name = _gen_table_replication_references(table, schema)
    cur = _get_rep_conn(credentials).cursor()
    if reset:
        drop_replication_slot(slot_name, cur)
        drop_publication(publication_name, cur)
    create_publication(publication_name, cur, publish)
    add_table_to_publication(table, schema, publication_name, cur)
    rep_slot = create_replication_slot(slot_name, cur)
    table_snapshot: DltResource = None
    if persist_snapshot:
        if rep_slot is None:
            logger.warning(
                "Cannot persist snapshot because it does not exist. "
                f'The replication slot "{slot_name}" already existed prior to calling this function.'
            )
        else:
            # need separate session to read the snapshot: https://stackoverflow.com/q/75852587
            cur_snap = _get_conn(credentials).cursor()
            snapshot_table_name = persist_snapshot_table(
                snapshot_name=rep_slot["snapshot_name"],
                table=table,
                schema=schema,
                cur=cur_snap,
                include_columns=include_columns,
            )
            table_snapshot = sql_table(
                credentials=credentials,
                table=snapshot_table_name,
                schema=schema,
            )
            table_snapshot.apply_hints(table_name=table)
    return (slot_name, publication_name, table_snapshot)


def create_publication(
    name: str,
    cur: ReplicationCursor,
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
    publication_name: str,
    cur: ReplicationCursor,
) -> None:
    """Adds a table to a publication for logical replication if the table is not a member yet.

    Raises error if the user is not owner of the table.
    """
    qual_name = _make_qualified_table_name(table_name, schema_name)
    esc_pub_name = escape_postgres_identifier(publication_name)
    try:
        cur.execute(f"ALTER PUBLICATION {esc_pub_name} ADD TABLE {qual_name};")
        logger.info(
            f"Successfully added table {qual_name} to publication {esc_pub_name}."
        )
    except psycopg2.errors.DuplicateObject:  # table is already member of publication
        pass


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
        logger.warning(f'Successfully dropped replication slot "{name}".')
    except psycopg2.errors.UndefinedObject:  # the replication slot does not exist
        logger.warning(
            f'Replication slot "{name}" cannot be dropped because it does not exist.'
        )


def drop_publication(name: str, cur: ReplicationCursor) -> None:
    """Drops a publication if it exists."""
    esc_name = escape_postgres_identifier(name)
    try:
        cur.execute(f"DROP PUBLICATION {esc_name};")
        cur.connection.commit()
        logger.warning(f"Successfully dropped publication {esc_name}.")
    except psycopg2.errors.UndefinedObject:  # the publication does not exist
        logger.warning(
            f"Publication {esc_name} cannot be dropped because it does not exist."
        )


def persist_snapshot_table(
    snapshot_name: str,
    table: str,
    schema: str,
    cur: psycopg2.extensions.cursor,
    include_columns: Optional[Sequence[str]] = None,
) -> str:
    """Persists exported snapshot table state.

    Reads snapshot table content and copies it into new table.
    """
    col_str = "*"
    if include_columns is not None:
        col_str = ", ".join(map(escape_postgres_identifier, include_columns))
    snapshot_table_name = f"{table}_snapshot_{snapshot_name}"
    snapshot_qual_name = _make_qualified_table_name(snapshot_table_name, schema)
    qual_name = _make_qualified_table_name(table, schema)
    cur.execute(
        f"""
        START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
        SET TRANSACTION SNAPSHOT '{snapshot_name}';
        CREATE TABLE {snapshot_qual_name} AS SELECT {col_str} FROM {qual_name};
    """
    )
    cur.connection.commit()
    logger.info(f"Successfully persisted snapshot table state in {snapshot_qual_name}.")
    return snapshot_table_name


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
        f"'{x}'" for xs in list(map(list, options.items())) for x in xs  # type: ignore[arg-type]
    )
    cur = _get_conn(credentials).cursor()
    cur.execute(
        "SELECT MAX(lsn) - '0/0' AS max_lsn "  # subtract '0/0' to convert pg_lsn type to int (https://stackoverflow.com/a/73738472)
        f"FROM pg_logical_slot_peek_binary_changes('{slot_name}', NULL, NULL, {options_str});"
    )
    lsn: int = cur.fetchone()[0]
    cur.connection.close()
    return lsn


def lsn_int_to_hex(lsn: int) -> str:
    """Convert integer LSN to postgres' hexadecimal representation."""
    # https://stackoverflow.com/questions/66797767/lsn-external-representation.
    hex_val = format(lsn, "x")
    if len(hex_val) > 8:
        return hex_val[:-8] + "/" + hex_val[-8:]
    return "0/" + format(lsn, "x").zfill(8)


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
        cur = _get_conn(credentials).cursor()
        cur.execute(
            f"SELECT * FROM pg_replication_slot_advance('{slot_name}', '{lsn_int_to_hex(upto_lsn)}');"
        )
        cur.connection.close()


def table_replication_items(
    credentials: ConnectionStringCredentials,
    slot_name: str,
    publication_name: str,
    include_columns: Optional[Sequence[str]] = None,
    upto_lsn: Optional[int] = None,
) -> Iterator[Union[TDataItem, DataItemWithMeta]]:
    """Yields data items from generator.

    Maintains LSN of last consumed message in state to track progress.
    At start of the run, advances the slot upto last consumed message in previous run.
    """
    # advance slot
    flush_lsn = dlt.current.resource_state().setdefault("last_commit_lsn", 0)
    advance_slot(flush_lsn, slot_name, credentials)

    # use max LSN in slot if `upto_lsn` was not provided
    options = {"publication_names": publication_name, "proto_version": "1"}
    if upto_lsn is None:
        upto_lsn = get_max_lsn(slot_name, options, credentials)

    # check if there's anything to consume
    if upto_lsn is not None and upto_lsn > flush_lsn:
        gen = ItemGenerator(credentials, slot_name, options, upto_lsn, include_columns)
        yield from gen
        dlt.current.resource_state()["last_commit_lsn"] = gen.last_commit_lsn


def _get_conn(
    credentials: ConnectionStringCredentials, connection_factory: Optional[Any] = None
) -> Union[psycopg2.extensions.connection, LogicalReplicationConnection]:
    # returns a psycopg2 connection
    return psycopg2.connect(  # type: ignore[no-any-return]
        database=credentials.database,
        user=credentials.username,
        password=credentials.password,
        host=credentials.host,
        port=credentials.port,
        connection_factory=connection_factory,
    )


def _get_rep_conn(
    credentials: ConnectionStringCredentials,
) -> LogicalReplicationConnection:
    # returns a psycopg2 LogicalReplicationConnection
    return _get_conn(credentials, LogicalReplicationConnection)  # type: ignore[return-value]


def _make_qualified_table_name(table_name: str, schema_name: str) -> str:
    return (
        escape_postgres_identifier(schema_name)
        + "."
        + escape_postgres_identifier(table_name)
    )


def _gen_table_replication_references(
    table_name: str, schema_name: str
) -> Tuple[str, str]:
    # generate replication slot and publication names dedicated to a single table
    slot_name = f"_dlt_slot_{schema_name}_{table_name}"
    publication_name = f"_dlt_pub_{schema_name}_{table_name}"
    return (slot_name, publication_name)


@dataclass
class ItemGenerator:
    credentials: ConnectionStringCredentials
    slot_name: str
    options: Dict[str, str]
    upto_lsn: Optional[int] = None
    include_columns: Optional[Sequence[str]] = None
    last_commit_lsn: Optional[int] = field(default=None, init=False)

    def __iter__(self) -> Iterator[Union[TDataItem, DataItemWithMeta]]:
        """Consumes messages from replication slot and generates data items.

        Does not advance the slot.
        Maintains LSN of last consumed Commit message in object state.
        """
        cur = _get_rep_conn(self.credentials).cursor()
        try:
            consumed_all = False
            consumer = MessageConsumer(self.upto_lsn, self.include_columns)
            cur.start_replication(
                slot_name=self.slot_name, decode=False, options=self.options
            )
            cur.consume_stream(consumer)
        except StopReplication:
            # all messages upto `upto_lsn` have been successfully consumed
            consumed_all = True
        finally:
            cur.connection.close()
            if consumed_all:
                for i in consumer.data_items:
                    yield i
                self.last_commit_lsn = consumer.last_commit_lsn


class MessageConsumer:
    """Consumes messages from a ReplicationCursor.

    Assumes all messages passed to __call__ have the same relation_id, i.e. they
    belong to the same table.
    """

    def __init__(
        self,
        upto_lsn: int,
        include_columns: Optional[Sequence[str]] = None,
    ) -> None:
        self.upto_lsn = upto_lsn
        self.include_columns = include_columns

        # data_items attribute maintains all data items
        self.data_items: List[Union[TDataItem, DataItemWithMeta]] = []
        # other attributes only maintain last-seen values
        self.last_table_schema: TTableSchema
        self.last_commit_ts: pendulum.DateTime
        self.last_commit_lsn = None

    def __call__(self, msg: ReplicationMessage) -> None:
        """Processes message received from stream.

        Breaks out of stream when `upto_lsn` is reached.
        """
        self.process_msg(msg)
        if msg.data_start == self.upto_lsn:
            raise StopReplication

    def process_msg(self, msg: ReplicationMessage) -> None:
        """Processes encoded replication message.

        Identifies message type and decodes accordingly.
        Message treatment is different for various message types.
        """
        op = (msg.payload[:1]).decode("utf-8")
        if op == "B":
            self.last_commit_ts = Begin(msg.payload).commit_ts
        elif op == "C":
            self.last_commit_lsn = msg.data_start
        elif op == "R":
            self.process_relation(Relation(msg.payload))
        elif op == "I":
            self.process_change(Insert(msg.payload), msg.data_start)
        elif op == "U":
            self.process_change(Update(msg.payload), msg.data_start)
        elif op == "D":
            self.process_change(Delete(msg.payload), msg.data_start)
        elif op == "T":
            logger.warning(
                "The truncate operation is currently not supported. "
                "Truncate replication messages are ignored."
            )

    def process_relation(self, decoded_msg: Relation) -> None:
        """Processes a replication message of type Relation.

        Stores table schema information from Relation message in object state
        and adds meta data item to the relation's list of data items to update
        the table schema.
        """
        # store table schema information
        columns = {c.name: _to_dlt_column_schema(c) for c in decoded_msg.columns}
        self.last_table_schema = {"name": decoded_msg.relation_name, "columns": columns}
        # add meta data item to update table schema
        meta_data_item = self.gen_meta_data_item(columns, self.include_columns)
        self.data_items.append(meta_data_item)

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
        data_item = self.gen_data_item(
            data=column_data,
            schema=self.last_table_schema["columns"],
            lsn=msg_start_lsn,
            commit_ts=self.last_commit_ts,
            for_delete=isinstance(decoded_msg, Delete),
            include_columns=self.include_columns,
        )
        self.data_items.append(data_item)

    @staticmethod
    def gen_data_item(
        data: List[ColumnData],
        schema: TTableSchemaColumns,
        lsn: int,
        commit_ts: pendulum.DateTime,
        for_delete: bool,
        include_columns: Optional[Sequence[str]] = None,
    ) -> TDataItem:
        """Generates data item from replication message data and corresponding metadata."""
        columns = zip(schema.values(), data)
        data_item = {
            schema["name"]: _to_dlt_val(
                data.col_data, schema["data_type"], data.col_data_category
            )
            for (schema, data) in columns
        }
        if include_columns is not None:
            data_item = {k: v for k, v in data_item.items() if k in include_columns}
        data_item["lsn"] = lsn
        if for_delete:
            data_item["deleted_ts"] = commit_ts
        return data_item

    @staticmethod
    def gen_meta_data_item(
        columns: TTableSchemaColumns,
        include_columns: Optional[Sequence[str]] = None,
    ) -> DataItemWithMeta:
        """Returns a data item containing only metadata, no data."""
        _columns = deepcopy(columns)
        if include_columns is not None:
            _columns = {k: v for k, v in columns.items() if k in include_columns}
        return dlt.mark.with_hints(
            [],
            # write disposition needs to be explicitly set, else it defaults to "append"
            dlt.mark.make_hints(write_disposition="merge", columns=_columns),
        )
