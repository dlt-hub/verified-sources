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
    Iterable,
    Tuple,
    Callable,
    TypedDict,
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
from dlt.common.schema.utils import merge_column, merge_table
from dlt.common.typing import TDataItem, TDataItems
from dlt.extract.items import DataItemWithMeta
from dlt.extract import DltSource, DltResource
from dlt.extract.resource import TResourceHints
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
@dlt.source
def init_replication(
    slot_name: str,
    schema: str,
    table_names: Union[str, Sequence[str]],
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    take_snapshots: bool = False,
    included_columns: Optional[Dict[str, TColumnNames]] = None,
    reset: bool = False,
) -> Iterable[DltResource]:
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
        return

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

    if isinstance(table_names, str):
        table_names = [table_names]

    for table in table_names:
        yield sql_table(  # type: ignore[name-defined]
            credentials=engine,
            table=table,
            schema=schema,
            included_columns=included_columns.get(table) if included_columns else None,
        )


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


def cleanup_snapshot_resources(snapshots: DltSource) -> None:
    """FIXME Awful hack to release the underlying SQL engine when snapshotting tables"""
    resources = snapshots.resources
    if resources:
        engine: Engine = next(iter(resources.values()))._explicit_args["credentials"]
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
        table_hints: Optional[Dict[str, TTableSchema]] = None,
    ) -> None:
        self.upto_lsn = upto_lsn
        self.table_qnames = table_qnames
        self.target_batch_size = target_batch_size

        self.included_columns = self._normalize_columns(included_columns)
        self.table_hints = self._normalize_hints(table_hints)

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
            if row_msg.op == Op.UNKNOWN:
                raise AssertionError(f"Unsupported operation : {row_msg}")

            if row_msg.op == Op.BEGIN:
                self.last_commit_ts = _epoch_micros_to_datetime(row_msg.commit_time)
            elif row_msg.op == Op.COMMIT:
                self.process_commit(msg.data_start)
            else:  # INSERT, UPDATE or DELETE
                self.process_change(row_msg, msg.data_start)
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
        """Processes replication message of type Insert, Update or Delete"""
        if msg.table not in self.table_qnames:
            return
        table_name = msg.table.split(".")[1]
        if msg.op == Op.DELETE:
            data_item = gen_data_item(msg)
        else:
            table_schema = self._get_table_schema(msg)
            data_item = gen_data_item(
                msg,
                included_columns=self.included_columns.get(table_name),
                column_schema=table_schema["columns"],
            )
        data_item["lsn"] = lsn
        self.data_items[table_name].append(data_item)

    def _get_table_schema(self, msg: RowMessage) -> TTableSchema:
        table_name = msg.table.split(".")[1]
        last_table_schema = self.last_table_schema.get(table_name)
        table_schema = infer_table_schema(
            msg,
            included_columns=self.included_columns.get(table_name),
            table_hints=self.table_hints.get(table_name),
        )
        if last_table_schema is None:
            self.last_table_schema[table_name] = table_schema
        elif last_table_schema != table_schema:
            raise StopReplication  # table schema change
        return table_schema

    @staticmethod
    def _normalize_columns(
        included_columns: Optional[Dict[str, TColumnNames]]
    ) -> Dict[str, Set[str]]:
        if not included_columns:
            return {}
        return {
            table_name: {
                col for col in ([columns] if isinstance(columns, str) else columns)
            }
            for table_name, columns in included_columns.items()
        }

    @staticmethod
    def _normalize_hints(
        table_hints: Optional[Dict[str, TTableSchema]]
    ) -> Dict[str, TTableSchema]:
        """Normalize the hints by ensuring that each table schema has a 'columns' TTableSchemaColumns."""
        if not table_hints:
            return {}
        for table_schema in table_hints.values():
            table_schema.setdefault("columns", {})
        return table_hints


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
    table_hints: Optional[Dict[str, TTableSchema]] = None
    last_commit_lsn: Optional[int] = field(default=None, init=False)
    generated_all: bool = False

    def __iter__(self) -> Iterator[Tuple[Union[str, TTableSchema], List[TDataItem]]]:
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
                table_hints=self.table_hints,
            )
            cur.consume_stream(consumer)
        except StopReplication:  # completed batch or reached `upto_lsn`
            pass
        finally:
            cur.connection.close()
            for table, data_items in consumer.data_items.items():
                # Yield schema if available; otherwise, yield the table name with items
                yield (consumer.last_table_schema.get(table, table), data_items)
            # Update state after flush
            self.last_commit_lsn = consumer.last_commit_lsn
            self.generated_all = consumer.consumed_all


def table_wal_handler(
    table: str,
) -> Callable[[TDataItem], Iterable[DataItemWithMeta]]:
    def handle(
        data_items: Tuple[Union[str, TTableSchema], List[TDataItem]]
    ) -> Iterable[DataItemWithMeta]:
        table_or_schema, items = data_items
        if isinstance(table_or_schema, Dict):
            table_name = table_or_schema["name"]
            if table_name == table:
                yield dlt.mark.with_hints(
                    [],
                    _table_to_resource_hints(table_or_schema),
                    create_table_variant=True,
                )
                yield dlt.mark.with_table_name(items, table)
        elif table_or_schema == table:
            yield dlt.mark.with_table_name(items, table)

    return handle


def _table_to_resource_hints(table_hints: TTableSchema) -> TResourceHints:
    return dlt.mark.make_hints(
        table_name=table_hints.get("name"),
        write_disposition=table_hints.get("write_disposition"),
        columns=table_hints.get("columns"),
        schema_contract=table_hints.get("schema_contract"),
        table_format=table_hints.get("table_format"),
        file_format=table_hints.get("file_format"),
    )


def infer_table_schema(
    msg: RowMessage,
    *,
    included_columns: Optional[Set[str]] = None,
    table_hints: Optional[TTableSchema] = None,
) -> TTableSchema:
    """Infers the table schema from the replication message and optional hints"""
    columns: TTableSchemaColumns = {
        col.column_name: _to_dlt_column_schema(col, col_info)
        for col, col_info in zip(msg.new_tuple, msg.new_typeinfo)
        if not included_columns or col.column_name in included_columns
    }

    # Add replication columns
    columns["lsn"] = {"data_type": "bigint", "nullable": True}
    columns["deleted_ts"] = {"data_type": "timestamp", "nullable": True}

    # write_disposition = (
    #     table_hints.get("write_disposition", "append") if table_hints else "append"
    # )
    #
    # FIXME if write_disposition not in ("replace", "append"):
    columns["lsn"]["dedup_sort"] = "desc"
    columns["deleted_ts"]["hard_delete"] = True

    schema, table = msg.table.split(".")
    table_schema: TTableSchema = {"name": table, "columns": columns}

    if table_hints:
        table_hints["name"] = table
        # FIXME I dont't know why I have to do this, but merge_table doesn't work right or I'm missing something
        col_hints = table_hints.get("columns")
        if col_hints:
            col_hints = {
                col_name: merge_column(columns[col_name], col_schema)
                for col_name, col_schema in col_hints.items()
                if not included_columns or col_name in included_columns
            }
        merge_table(schema, table_schema, table_hints)

    return table_schema


def gen_data_item(
    msg: RowMessage,
    *,
    included_columns: Optional[Set[str]] = None,
    column_schema: Optional[TTableSchemaColumns] = None,
) -> TDataItem:
    """Generates data item from a row message and corresponding metadata."""
    data_item: TDataItem = {}
    if msg.op != Op.DELETE:
        row = msg.new_tuple
    else:
        row = msg.old_tuple
        data_item["deleted_ts"] = _epoch_micros_to_datetime(msg.commit_time)

    for data in row:
        col_name = data.column_name
        if included_columns and col_name not in included_columns:
            continue
        data_type = (
            column_schema[col_name]["data_type"]
            if column_schema and column_schema.get(col_name)
            else data.column_type
        )
        data_item[col_name] = _to_dlt_val(
            data, data_type, for_delete=msg.op == Op.DELETE
        )

    return data_item
