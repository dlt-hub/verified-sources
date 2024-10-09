from typing import (
    Optional,
    Dict,
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
from dlt.common.schema.typing import (
    TTableSchema,
    TTableSchemaColumns,
    TColumnNames,
    TWriteDisposition,
)
from dlt.common.schema.utils import merge_column
from dlt.common.data_writers.escape import escape_postgres_identifier
from dlt.extract.items import DataItemWithMeta
from dlt.extract.resource import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from .schema_types import _to_dlt_column_schema, _to_dlt_val
from .exceptions import IncompatiblePostgresVersionException
from .decoders import (
    Begin,
    Relation,
    Insert,
    Update,
    Delete,
    ColumnData,
    convert_pg_ts,
)

from sqlalchemy import Connection as ConnectionSqla, Engine, event

from .pg_logicaldec_pb2 import Op, RowMessage  # type: ignore [attr-defined]
from .schema_types import _to_dlt_column_schema, _to_dlt_val
from .exceptions import SqlDatabaseSourceImportError
from google.protobuf.json_format import MessageToDict


@dlt.sources.config.with_config(sections=("sources", "pg_legacy_replication"))
def init_replication(
    slot_name: str,
    schema: Optional[str] = dlt.config.value,
    table_names: Optional[List[str]] = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    take_snapshots: bool = True,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    reset: bool = False,
) -> Optional[List[DltResource]]:
    """Initializes replication for one, several, or all tables within a schema.

    Can be called repeatedly with the same `slot_name`:
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
        schema (str): Name of the schema to replicate tables from.
        table_names (Optional[Union[str, Sequence[str]]]):  Name(s) of the table(s)
          to include in the publication. If not provided, all tables in the schema
          are included (also tables added to the schema after the publication was created).
        credentials (ConnectionStringCredentials): Postgres database credentials.
        take_snapshots (bool): Whether the table states in the snapshot exported
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
          Argument is only used if `take_snapshots` is `True`.
          ```
          Argument is only used if `take_snapshots` is `True`.
        reset (bool): If set to True, the existing slot and publication are dropped
          and recreated. Has no effect if a slot and publication with the provided
          names do not yet exist.

    Returns:
        - None if `take_snapshots` is `False`
        - a `DltResource` object or a list of `DltResource` objects for the snapshot
          table(s) if `take_snapshots` is `True` and the replication slot did not yet exist
    """
    rep_conn = _get_rep_conn(credentials)
    rep_cur = rep_conn.cursor()
    if reset:
        drop_replication_slot(slot_name, rep_cur)
    slot = create_replication_slot(slot_name, rep_cur)

    # Close connection if no snapshots are needed
    if not take_snapshots:
        rep_conn.close()
        return None

    # Ensure `sqlalchemy` and `sql_table` are available
    _import_sql_table_resource()
    engine = _configure_engine(credentials, rep_conn)

    @event.listens_for(engine, "begin")
    def on_begin(conn: ConnectionSqla) -> None:
        cur = conn.connection.cursor()
        if slot is None:
            # Using the same isolation level that pg_backup uses
            cur.execute(
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE"
            )
        else:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute(f"SET TRANSACTION SNAPSHOT '{slot['snapshot_name']}'")

    include_columns = include_columns or {}
    return [
        sql_table(  # type: ignore[name-defined]
            credentials=engine,
            table=table_name,
            schema=schema,
            included_columns=include_columns.get(table_name),
        )
        for table_name in table_names
    ]


def _configure_engine(
    credentials: ConnectionStringCredentials, rep_conn: LogicalReplicationConnection
) -> Engine:
    """
    Configures the SQLAlchemy engine.
    Also attaches the replication connection in order to prevent it being garbage collected and closed.
    """
    engine: Engine = engine_from_credentials(credentials, may_dispose_after_use=False)  # type: ignore[name-defined]
    engine.execution_options(stream_results=True, max_row_buffer=2 * 50000)
    setattr(engine, "rep_conn", rep_conn)  # noqa
    return engine


@dlt.sources.config.with_config(sections=("sources", "pg_legacy_replication"))
def get_pg_version(
    cur: cursor = None,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
) -> int:
    """Returns Postgres server version as int."""
    if cur is not None:
        return cur.connection.server_version
    return _get_conn(credentials).server_version


def create_replication_slot(  # type: ignore[return]
    name: str, cur: ReplicationCursor, output_plugin: str = "decoderbufs"
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


# def persist_snapshot_table(
#     snapshot_name: str,
#     table_name: str,
#     schema_name: str,
#     cur: cursor,
#     include_columns: Optional[Sequence[str]] = None,
# ) -> str:
#     """Persists exported snapshot table state.
#
#     Reads snapshot table content and copies it into new table.
#     """
#     col_str = "*"
#     if include_columns is not None:
#         col_str = ", ".join(map(escape_postgres_identifier, include_columns))
#     snapshot_table_name = f"{table_name}_snapshot_{snapshot_name}"
#     snapshot_qual_name = _make_qualified_table_name(snapshot_table_name, schema_name)
#     qual_name = _make_qualified_table_name(table_name, schema_name)
#     cur.execute(
#         f"""
#         START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
#         SET TRANSACTION SNAPSHOT '{snapshot_name}';
#         CREATE TABLE {snapshot_qual_name} AS SELECT {col_str} FROM {qual_name};
#     """
#     )
#     cur.connection.commit()
#     logger.info(f"Successfully persisted snapshot table state in {snapshot_qual_name}.")
#     return snapshot_table_name
#
#
# def snapshot_table_resource(
#     snapshot_table_name: str,
#     schema_name: str,
#     table_name: str,
#     write_disposition: TWriteDisposition,
#     columns: TTableSchemaColumns = None,
#     credentials: ConnectionStringCredentials = dlt.secrets.value,
# ) -> DltResource:
#     """Returns a resource for a persisted snapshot table.
#
#     Can be used to perform an initial load of the table, so all data that
#     existed in the table prior to initializing replication is also captured.
#     """
#     resource: DltResource = sql_table(  # type: ignore[name-defined]
#         credentials=credentials,
#         table=snapshot_table_name,
#         schema=schema_name,
#         detect_precision_hints=True,
#     )
#     primary_key = _get_pk(table_name, schema_name, credentials)
#     resource.apply_hints(
#         table_name=table_name,
#         write_disposition=write_disposition,
#         columns=columns,
#         primary_key=primary_key,
#     )
#     return resource


def get_max_lsn(
    slot_name: str,
    credentials: ConnectionStringCredentials,
) -> Optional[int]:
    """Returns maximum Log Sequence Number (LSN) in replication slot.

    Returns None if the replication slot is empty.
    Does not consume the slot, i.e. messages are not flushed.
    Raises error if the replication slot or publication does not exist.
    """
    cur = _get_conn(credentials).cursor()
    cur.execute(
        "SELECT MAX(lsn) - '0/0' AS max_lsn "  # subtract '0/0' to convert pg_lsn type to int (https://stackoverflow.com/a/73738472)
        f"FROM pg_logical_slot_peek_binary_changes('{slot_name}', NULL, NULL);"
    )
    lsn: int = cur.fetchone()[0]
    cur.connection.close()
    return lsn


# def get_pub_ops(
#     pub_name: str,
#     credentials: ConnectionStringCredentials,
# ) -> Dict[str, bool]:
#     """Returns dictionary of DML operations and their publish status."""
#     cur = _get_conn(credentials).cursor()
#     cur.execute(
#         f"""
#         SELECT pubinsert, pubupdate, pubdelete, pubtruncate
#         FROM pg_publication WHERE pubname = '{pub_name}'
#     """
#     )
#     result = cur.fetchone()
#     cur.connection.close()
#     if result is None:
#         raise ValueError(f'Publication "{pub_name}" does not exist.')
#     return {
#         "insert": result[0],
#         "update": result[1],
#         "delete": result[2],
#         "truncate": result[3],
#     }


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
        cur = _get_conn(credentials).cursor()
        cur.execute(
            f"SELECT * FROM pg_replication_slot_advance('{slot_name}', '{lsn_int_to_hex(upto_lsn)}');"
        )
        cur.connection.close()


def _import_sql_table_resource() -> None:
    """Imports external `sql_table` resource from `sql_database` source.

    Raises error if `sql_database` source is not available.
    """
    global sql_table, engine_from_credentials
    try:
        from ..sql_database import sql_table, engine_from_credentials  # type: ignore[import-untyped]
    except ImportError:
        try:
            from dlt.sources.sql_database import sql_table, engine_from_credentials  # type: ignore[import-not-found]
        except ImportError:
            try:
                from sql_database import sql_table, engine_from_credentials
            except ImportError as e:
                raise SqlDatabaseSourceImportError from e


def _get_conn(
    credentials: ConnectionStringCredentials,
    connection_factory: Optional[Any] = None,
) -> ConnectionExt:
    """Returns a psycopg2 connection to interact with postgres."""
    return psycopg2.connect(  # type: ignore[no-any-return]
        database=credentials.database,
        user=credentials.username,
        password=credentials.password,
        host=credentials.host,
        port=credentials.port,
        connection_factory=connection_factory,
        **({} if credentials.query is None else credentials.query),
    )


def _get_rep_conn(
    credentials: ConnectionStringCredentials,
) -> LogicalReplicationConnection:
    """Returns a psycopg2 LogicalReplicationConnection to interact with postgres replication functionality.

    Raises error if the user does not have the REPLICATION attribute assigned.
    """
    return _get_conn(credentials, LogicalReplicationConnection)  # type: ignore[return-value]


# def _make_qualified_table_name(table_name: str, schema_name: str) -> str:
#     """Escapes and combines a schema and table name."""
#     return (
#         escape_postgres_identifier(schema_name)
#         + "."
#         + escape_postgres_identifier(table_name)
#     )
#
#
# def _get_pk(
#     table_name: str,
#     schema_name: str,
#     credentials: ConnectionStringCredentials,
# ) -> Optional[TColumnNames]:
#     """Returns primary key column(s) for postgres table.
#
#     Returns None if no primary key columns exist.
#     """
#     qual_name = _make_qualified_table_name(table_name, schema_name)
#     cur = _get_conn(credentials).cursor()
#     # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
#     cur.execute(
#         f"""
#         SELECT a.attname
#         FROM   pg_index i
#         JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
#         WHERE  i.indrelid = '{qual_name}'::regclass
#         AND    i.indisprimary;
#     """
#     )
#     result = [tup[0] for tup in cur.fetchall()]
#     cur.connection.close()
#     if len(result) == 0:
#         return None
#     elif len(result) == 1:
#         return result[0]  # type: ignore[no-any-return]
#     return result


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
            cur.start_replication(
                slot_name=self.slot_name,
                start_lsn=self.start_lsn,
                decode=False,
                options=self.options,
            )
            pub_opts = {
                "insert": True,
                "update": True,
                "delete": True,
                "truncate": False,
            }
            consumer = MessageConsumer(
                upto_lsn=self.upto_lsn,
                pub_ops=pub_opts,
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
                yield data_items[0]  # meta item with column hints only, no data
                yield dlt.mark.with_table_name(data_items[1:], table_name)
            self.generated_all = consumer.consumed_all


from devtools import debug


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
        row_msg = RowMessage()
        row_msg.ParseFromString(msg.payload)

        debug(MessageToDict(row_msg, including_default_value_fields=True))  # type: ignore[call-arg]
        op = row_msg.op
        if op == Op.BEGIN:
            self.last_commit_ts = convert_pg_ts(row_msg.commit_time)  # type: ignore[assignment]
        # if op == Op.UPDATE:
        #     self.process_change(row_msg)
        # op = msg.payload[:1]
        # if op == b"I":
        #     self.process_change(Insert(msg.payload), msg.data_start)
        # elif op == b"U":
        #     self.process_change(Update(msg.payload), msg.data_start)
        # elif op == b"D":
        #     self.process_change(Delete(msg.payload), msg.data_start)
        # elif op == b"B":
        #     self.last_commit_ts = Begin(msg.payload).commit_ts  # type: ignore[assignment]
        # elif op == b"C":
        #     self.process_commit(msg)
        # elif op == b"R":
        #     self.process_relation(Relation(msg.payload))
        # elif op == b"T":
        #     logger.warning(
        #         "The truncate operation is currently not supported. "
        #         "Truncate replication messages are ignored."
        #     )
        else:
            raise AssertionError(f"Unsupported operation : {row_msg}")

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
