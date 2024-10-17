from typing import (
    Optional,
    Dict,
    Set,
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

from .pg_logicaldec_pb2 import Op, RowMessage
from .schema_types import _to_dlt_column_schema, _to_dlt_val
from .exceptions import SqlDatabaseSourceImportError
from google.protobuf.json_format import MessageToDict
from collections import defaultdict


@dlt.sources.config.with_config(sections=("sources", "pg_legacy_replication"))
def init_replication(
    slot_name: str,
    schema: str = dlt.config.value,
    table_names: Sequence[str] = dlt.config.value,
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

    @event.listens_for(engine, "engine_disposed")
    def on_engine_disposed(engine: Engine) -> None:
        delattr(engine, "rep_conn")

    return engine


def cleanup_snapshot_resources(snapshots: List[DltResource]) -> None:
    """FIXME Awful hack to release the underlying SQL engine when snapshotting tables"""
    if not snapshots:
        return
    engine: Engine = snapshots[0]._explicit_args["credentials"]
    engine.dispose()


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


@dataclass
class ItemGenerator:
    credentials: ConnectionStringCredentials
    slot_name: str
    schema: str
    table_names: Sequence[str]
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
                schema=self.schema,
                table_names=set(self.table_names),
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
            for table_name, data_items in consumer.data_items.items():
                table_schema = consumer.last_table_schema[table_name]
                assert table_name == table_schema["name"]
                yield dlt.mark.with_hints(  # meta item with column hints only, no data
                    [],
                    dlt.mark.make_hints(
                        table_name=table_name, columns=table_schema["columns"]
                    ),
                    create_table_variant=True,
                )
                yield dlt.mark.with_table_name(data_items, table_name)
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
        schema: str,
        table_names: Set[str],
        target_batch_size: int = 1000,
        include_columns: Optional[Dict[str, Sequence[str]]] = None,
        columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    ) -> None:
        self.upto_lsn = upto_lsn
        self.pub_ops = pub_ops
        self.schema = schema
        self.table_names = table_names
        self.target_batch_size = target_batch_size
        self.include_columns = include_columns
        self.columns = columns

        self.consumed_all: bool = False
        # data_items attribute maintains all data items
        self.data_items: Dict[
            str, List[Union[TDataItem, DataItemWithMeta]]
        ] = defaultdict(
            list
        )  # maps qualified table names to list of data items
        # other attributes only maintain last-seen values
        self.last_table_schema: Dict[
            str, TTableSchema
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
        op = row_msg.op
        if op == Op.BEGIN:
            self.last_commit_ts = convert_pg_ts(row_msg.commit_time)  # type: ignore[assignment]
        elif op == Op.COMMIT:
            self.process_commit(msg)
        elif op == Op.INSERT:
            schema, table_name = row_msg.table.split(".")
            if schema != self.schema or table_name not in self.table_names:
                return
            last_table_schema = self.last_table_schema.get(table_name)
            table_schema = extract_table_schema(row_msg)
            if last_table_schema is None:
                self.last_table_schema[table_name] = table_schema
            elif last_table_schema != table_schema:
                raise StopReplication  # table schema change
            data_item = gen_data_item(
                row_msg,
                table_schema["columns"],
                lsn=msg.data_start,
                include_columns=(
                    None
                    if self.include_columns is None
                    else self.include_columns.get(table_name)
                ),
            )
            self.data_items[table_name].append(data_item)
        # if op == Op.UPDATE:
        #     self.process_change(row_msg)
        # op = msg.payload[:1]
        # if op == b"I":
        #     self.process_change(Insert(msg.payload), msg.data_start)
        # elif op == b"U":
        #     self.process_change(Update(msg.payload), msg.data_start)
        # elif op == b"D":
        #     self.process_change(Delete(msg.payload), msg.data_start)
        # elif op == b"R":
        #     self.process_relation(Relation(msg.payload))
        # elif op == b"T":
        #     logger.warning(
        #         "The truncate operation is currently not supported. "
        #         "Truncate replication messages are ignored."
        #     )
        else:
            debug(msg)
            debug(MessageToDict(row_msg, including_default_value_fields=True))  # type: ignore[call-arg]
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

    # def process_relation(self, decoded_msg: Relation) -> None:
    #     """Processes a replication message of type Relation.
    #
    #     Stores table schema in object state.
    #     Creates meta item to emit column hints while yielding data.
    #
    #     Raises StopReplication when a table's schema changes.
    #     """
    #     if (
    #         self.data_items.get(decoded_msg.relation_id) is not None
    #     ):  # table schema change
    #         raise StopReplication
    #     # get table schema information from source and store in object state
    #     table_name = decoded_msg.relation_name
    #     columns: TTableSchemaColumns = {
    #         c.name: _to_dlt_column_schema(c) for c in decoded_msg.columns
    #     }
    #     self.last_table_schema[decoded_msg.relation_id] = {
    #         "name": table_name,
    #         "columns": columns,
    #     }
    #
    #     # apply user input
    #     # 1) exclude columns
    #     include_columns = (
    #         None
    #         if self.include_columns is None
    #         else self.include_columns.get(table_name)
    #     )
    #     if include_columns is not None:
    #         columns = {k: v for k, v in columns.items() if k in include_columns}
    #     # 2) override source hints
    #     column_hints: TTableSchemaColumns = (
    #         dict() if self.columns is None else self.columns.get(table_name, dict())
    #     )
    #     for column_name, column_val in column_hints.items():
    #         columns[column_name] = merge_column(columns[column_name], column_val)
    #
    #     # add hints for replication columns
    #     columns["lsn"] = {"data_type": "bigint", "nullable": True}
    #     if self.pub_ops["update"] or self.pub_ops["delete"]:
    #         columns["lsn"]["dedup_sort"] = "desc"
    #     if self.pub_ops["delete"]:
    #         columns["deleted_ts"] = {
    #             "hard_delete": True,
    #             "data_type": "timestamp",
    #             "nullable": True,
    #         }
    #
    #     # determine write disposition
    #     write_disposition: TWriteDisposition = "append"
    #     if self.pub_ops["update"] or self.pub_ops["delete"]:
    #         write_disposition = "merge"
    #
    #     # include meta item to emit hints while yielding data
    #     meta_item = dlt.mark.with_hints(
    #         [],
    #         dlt.mark.make_hints(
    #             table_name=table_name,
    #             write_disposition=write_disposition,
    #             columns=columns,
    #         ),
    #         create_table_variant=True,
    #     )
    #     self.data_items[decoded_msg.relation_id] = [meta_item]

    # def process_change(
    #     self, decoded_msg: Union[Insert, Update, Delete], msg_start_lsn: int
    # ) -> None:
    #     """Processes replication message of type Insert, Update, or Delete.
    #
    #     Adds data item for inserted/updated/deleted record to instance attribute.
    #     """
    #     if isinstance(decoded_msg, (Insert, Update)):
    #         column_data = decoded_msg.new_tuple.column_data
    #     elif isinstance(decoded_msg, Delete):
    #         column_data = decoded_msg.old_tuple.column_data
    #     table_name = self.last_table_schema[decoded_msg.relation_id]["name"]
    #     data_item = self.gen_data_item(
    #         data=column_data,
    #         column_schema=self.last_table_schema[decoded_msg.relation_id]["columns"],
    #         lsn=msg_start_lsn,
    #         commit_ts=self.last_commit_ts,
    #         for_delete=isinstance(decoded_msg, Delete),
    #         include_columns=(
    #             None
    #             if self.include_columns is None
    #             else self.include_columns.get(table_name)
    #         ),
    #     )
    #     self.data_items[decoded_msg.relation_id].append(data_item)
    #
    # @staticmethod
    # def gen_data_item(
    #     data: List[ColumnData],
    #     column_schema: TTableSchemaColumns,
    #     lsn: int,
    #     commit_ts: pendulum.DateTime,
    #     for_delete: bool,
    #     include_columns: Optional[Sequence[str]] = None,
    # ) -> TDataItem:
    #     """Generates data item from replication message data and corresponding metadata."""
    #     data_item = {
    #         schema["name"]: _to_dlt_val(
    #             val=data.col_data,
    #             data_type=schema["data_type"],
    #             byte1=data.col_data_category,
    #             for_delete=for_delete,
    #         )
    #         for (schema, data) in zip(column_schema.values(), data)
    #         if (True if include_columns is None else schema["name"] in include_columns)
    #     }
    #     data_item["lsn"] = lsn
    #     if for_delete:
    #         data_item["deleted_ts"] = commit_ts
    #     return data_item


# FIXME Refactor later
from .schema_types import _PG_TYPES, _type_mapper
from dlt.common.schema.typing import TColumnType, TColumnSchema

_DATUM_PRECISIONS: Dict[str, int] = {
    "datum_int32": 32,
    "datum_int64": 64,
    "datum_float": 32,
    "datum_double": 64,
}
"""TODO: Add comment here"""


def extract_table_schema(
    row_msg: RowMessage, *, include_columns: Optional[Sequence[str]] = None
) -> TTableSchema:
    columns: TTableSchemaColumns = {}
    type_mapper = _type_mapper()
    for col, col_info in zip(row_msg.new_tuple, row_msg.new_typeinfo):
        col_name = col.column_name
        if include_columns is not None and col_name not in include_columns:
            continue
        assert (
            _PG_TYPES[col.column_type] == col_info.modifier
        ), f"Type mismatch for column {col_name}"
        col_type: TColumnType = type_mapper.from_db_type(col_info.modifier)
        col_schema: TColumnSchema = {
            "name": col_name,
            "nullable": col_info.value_optional,
            **col_type,
        }

        precision = _DATUM_PRECISIONS.get(col.WhichOneof("datum"))
        if precision is not None:
            col_schema["precision"] = precision

        columns[col_name] = col_schema

    # Add replication columns
    columns.update(
        {
            "lsn": {
                "data_type": "bigint",
                "nullable": True,
                "dedup_sort": "desc",
            },
            "deleted_ts": {
                "data_type": "timestamp",
                "nullable": True,
                "hard_delete": True,
            },
        }
    )

    table_name = row_msg.table.split(".")[1]
    return {"name": table_name, "columns": columns}


def gen_data_item(
    row_msg: RowMessage,
    column_schema: TTableSchemaColumns,
    *,
    lsn: int,
    include_columns: Optional[Sequence[str]] = None,
) -> TDataItem:
    """Generates data item from a `RowMessage` and corresponding metadata."""
    assert row_msg.op in (Op.INSERT, Op.UPDATE, Op.DELETE)
    column_data = (
        row_msg.new_tuple if row_msg.op in (Op.INSERT, Op.UPDATE) else row_msg.old_tuple
    )

    data_item = {
        data.column_name: getattr(data, data.WhichOneof("datum"))
        for schema, data in zip(column_schema.values(), column_data)
        if include_columns is None or data.column_name in include_columns
    }

    data_item["lsn"] = lsn
    if row_msg.op == Op.DELETE:
        data_item["deleted_ts"] = _convert_pg_timestamp(row_msg.commit_time)

    return data_item


def _convert_pg_timestamp(microseconds_since_2000: int) -> pendulum.DateTime:
    epoch_2000 = pendulum.datetime(2000, 1, 1, tz="UTC")
    return epoch_2000.add(microseconds=microseconds_since_2000)
