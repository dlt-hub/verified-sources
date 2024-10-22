from collections import defaultdict
from dataclasses import dataclass, field
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

import dlt
import psycopg2
from dlt.common import logger
from dlt.common.pendulum import pendulum
from dlt.common.schema.typing import (
    TColumnNames,
    TTableSchema,
    TTableSchemaColumns,
)
from dlt.common.schema.utils import merge_column
from dlt.common.typing import TDataItem
from dlt.extract.items import DataItemWithMeta
from dlt.extract.resource import DltResource
from dlt.sources.credentials import ConnectionStringCredentials
from psycopg2.extensions import cursor, connection as ConnectionExt
from psycopg2.extras import (
    LogicalReplicationConnection,
    ReplicationCursor,
    ReplicationMessage,
    StopReplication,
)
from sqlalchemy import Connection as ConnectionSqla, Engine, event

from .exceptions import SqlDatabaseSourceImportError
from .pg_logicaldec_pb2 import DatumMessage, Op, RowMessage
from .schema_types import _epoch_micros_to_datetime, _to_dlt_column_schema, _to_dlt_val


@dlt.sources.config.with_config(sections=("sources", "pg_legacy_replication"))
def init_replication(
    slot_name: str,
    schema: str = dlt.config.value,
    table_names: List[str] = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    take_snapshots: bool = False,
    included_columns: Optional[Dict[str, TColumnNames]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
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
        included_columns (Optional[Dict[str, Sequence[str]]]): Maps table name(s) to
          sequence of names of columns to include in the snapshot table(s).
          Any column not in the sequence is excluded. If not provided, all columns
          are included. For example:
          ```
          included_columns={
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

    included_columns = included_columns or {}
    columns = columns or {}
    return [
        _prepare_snapshot_resource(
            engine,
            table_name,
            schema,
            included_columns=included_columns.get(table_name),
            columns=columns.get(table_name),
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


def _prepare_snapshot_resource(
    engine: Engine,
    table_name: str,
    schema: str,
    *,
    included_columns: Optional[TColumnNames] = None,
    columns: Optional[TTableSchemaColumns] = None,
) -> DltResource:
    t_rsrc: DltResource = sql_table(  # type: ignore[name-defined]
        credentials=engine,
        table=table_name,
        schema=schema,
        included_columns=included_columns,
    )
    if columns:
        t_rsrc.apply_hints(columns=columns)
    return t_rsrc


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


class MessageConsumer:
    """Consumes messages from a ReplicationCursor sequentially.

    Generates data item for each `insert`, `update`, and `delete` message.
    Processes in batches to limit memory usage.
    Maintains message data needed by subsequent messages in internal state.
    """

    def __init__(
        self,
        upto_lsn: int,
        table_qnames: Set[str],
        target_batch_size: int = 1000,
        included_columns: Optional[Dict[str, TColumnNames]] = None,
        columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    ) -> None:
        self.upto_lsn = upto_lsn
        self.table_qnames = table_qnames
        self.target_batch_size = target_batch_size
        self.included_columns = (
            {
                table_name: _normalize_included_columns(columns)
                for table_name, columns in included_columns.items()
            }
            if included_columns
            else {}
        )
        self.columns = columns or {}

        self.consumed_all: bool = False
        # maps table names to list of data items
        self.data_items: Dict[str, List[TDataItem]] = defaultdict(list)
        # maps table name to table schema
        self.last_table_schema: Dict[str, TTableSchema] = dict()
        self.last_commit_ts: pendulum.DateTime
        self.last_commit_lsn: int

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
        try:
            row_msg.ParseFromString(msg.payload)
            op = row_msg.op
            lsn = msg.data_start
            if op == Op.BEGIN:
                self.last_commit_ts = _epoch_micros_to_datetime(row_msg.commit_time)
            elif op == Op.COMMIT:
                self.process_commit(lsn)
            elif op == Op.INSERT:
                self.process_change(row_msg, lsn)
            elif op == Op.UPDATE:
                self.process_change(row_msg, lsn)
            elif op == Op.DELETE:
                self.process_delete(row_msg, lsn)
            else:
                raise AssertionError(f"Unsupported operation : {row_msg}")
        except StopReplication:
            raise
        except Exception:
            logger.error(
                "A fatal error occured while processing a message: %s", row_msg
            )
            raise

    def process_commit(self, lsn: int) -> None:
        """Updates object state when Commit message is observed.

        Raises StopReplication when `upto_lsn` or `target_batch_size` is reached.
        """
        self.last_commit_lsn = lsn
        if lsn >= self.upto_lsn:
            self.consumed_all = True
        n_items = sum(
            [len(items) for items in self.data_items.values()]
        )  # combine items for all tables
        if self.consumed_all or n_items >= self.target_batch_size:
            raise StopReplication

    def process_change(self, msg: RowMessage, lsn: int) -> None:
        """Processes replication message of type Insert or Update"""
        if msg.table not in self.table_qnames:
            return
        _, table_name = msg.table.split(".")
        last_table_schema = self.last_table_schema.get(table_name)
        table_schema = infer_table_schema(
            msg,
            column_hints=self.columns.get(table_name),
            included_columns=self.included_columns.get(table_name),
        )
        if last_table_schema is None:
            self.last_table_schema[table_name] = table_schema
        elif last_table_schema != table_schema:
            raise StopReplication  # table schema change

        data_item = gen_data_item(
            msg.new_tuple,
            column_schema=table_schema["columns"],
            included_columns=self.included_columns.get(table_name),
        )
        data_item["lsn"] = lsn
        self.data_items[table_name].append(data_item)

    def process_delete(self, msg: RowMessage, lsn: int) -> None:
        """Processes replication message of type Delete"""
        if msg.table not in self.table_qnames:
            return
        _, table_name = msg.table.split(".")
        data_item = gen_data_item(
            msg.old_tuple,
            for_delete=True,
            included_columns=self.included_columns.get(table_name),
        )
        data_item["lsn"] = lsn
        data_item["deleted_ts"] = _epoch_micros_to_datetime(msg.commit_time)
        self.data_items[table_name].append(data_item)


@dataclass
class ItemGenerator:
    credentials: ConnectionStringCredentials
    slot_name: str
    table_qnames: Set[str]
    options: Dict[str, str]
    upto_lsn: int
    start_lsn: int = 0
    target_batch_size: int = 1000
    included_columns: Optional[Dict[str, TColumnNames]] = None
    columns: Optional[Dict[str, TTableSchemaColumns]] = None
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
            consumer = MessageConsumer(
                upto_lsn=self.upto_lsn,
                table_qnames=self.table_qnames,
                target_batch_size=self.target_batch_size,
                included_columns=self.included_columns,
                columns=self.columns,
            )
            cur.consume_stream(consumer)
        except StopReplication:  # completed batch or reached `upto_lsn`
            pass
        finally:
            cur.connection.close()
            yield from self.flush(consumer)

    def flush(
        self, consumer: MessageConsumer
    ) -> Iterator[Union[TDataItem, DataItemWithMeta]]:
        self.last_commit_lsn = consumer.last_commit_lsn
        for table_name, data_items in consumer.data_items.items():
            table_schema = consumer.last_table_schema.get(table_name)
            if table_schema:
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


def infer_table_schema(
    msg: RowMessage,
    *,
    column_hints: Optional[TTableSchemaColumns] = None,
    included_columns: Optional[Set[str]] = None,
) -> TTableSchema:
    """Infers the table schema from the replication message and optional hints"""
    columns: TTableSchemaColumns = {
        col.column_name: (
            merge_column(
                _to_dlt_column_schema(col, col_info),
                column_hints.get(col.column_name),
            )
            if column_hints and col.column_name in column_hints
            else _to_dlt_column_schema(col, col_info)
        )
        for col, col_info in zip(msg.new_tuple, msg.new_typeinfo)
        if not included_columns or col.column_name in included_columns
    }

    columns["lsn"] = {"data_type": "bigint", "nullable": True, "dedup_sort": "desc"}
    columns["deleted_ts"] = {
        "data_type": "timestamp",
        "nullable": True,
        "hard_delete": True,
    }

    return {
        "name": msg.table.split(".")[1],
        "columns": columns,
    }


def gen_data_item(
    row: Sequence[DatumMessage],
    *,
    included_columns: Optional[Set[str]] = None,
    column_schema: Optional[TTableSchemaColumns] = None,
    for_delete: bool = False,
) -> TDataItem:
    """Generates data item from a row and corresponding metadata."""
    data_item: TDataItem = {}

    for data in row:
        col_name = data.column_name
        if included_columns and col_name not in included_columns:
            continue
        data_type = (
            column_schema[col_name]["data_type"] if column_schema else data.column_type
        )
        data_item[col_name] = _to_dlt_val(data, data_type, for_delete=for_delete)

    return data_item


def _normalize_included_columns(
    included_columns: Optional[TColumnNames],
) -> Optional[Set[str]]:
    if included_columns is None:
        return None
    return (
        {included_columns}
        if isinstance(included_columns, str)
        else set(included_columns)
    )
