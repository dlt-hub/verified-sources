"""Initial snapshot of replicated tables, consistent with the start of the replication slot."""

import weakref
from typing import Any, Callable, Dict, Iterator, List, Optional, Type

import sqlalchemy as sa
from psycopg2.extensions import new_array_type, new_type, register_type
from psycopg2.extras import LogicalReplicationConnection
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.types import TypeEngine

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem
from dlt.extract import DltResource
from dlt.sources.sql_database.helpers import TableLoader

from .schema_types import add_session_options

# sqlalchemy < 2 reflects domains as their base type
_PG_DOMAIN = getattr(postgresql, "DOMAIN", ())

# interval values have no dlt data type and are loaded as text, the same as replicated values
_INTERVAL_AS_TEXT = new_type(
    (1186,), "PG_REPLICATION_INTERVAL", lambda value, cur: value
)
_INTERVAL_ARRAY_AS_TEXT = new_array_type(
    (1187,), "PG_REPLICATION_INTERVAL_ARRAY", _INTERVAL_AS_TEXT
)


def snapshot_type_adapter(sql_type: TypeEngine) -> TypeEngine:  # type: ignore[type-arg]
    """Maps snapshot column types like replicated columns.

    Enums become text without length, so labels added later fit. Domains become their base type.
    """
    if isinstance(sql_type, sqltypes.Enum):
        return sqltypes.Text()
    if isinstance(sql_type, _PG_DOMAIN):
        return sql_type.data_type  # type: ignore[attr-defined,no-any-return]
    return sql_type


def remove_not_null_adapter(table: sa.Table) -> sa.Table:
    """Reflects non-key columns as nullable, as replicated columns are."""
    for column in table.columns:
        if not column.primary_key:
            column.nullable = True
    return table


def ensure_sqlalchemy_2() -> None:
    """Raises on SQLAlchemy 1.4, which does not fire `begin` events that set the snapshot."""
    if int(sa.__version__.split(".")[0]) < 2:
        raise MissingDependencyException(
            "pg_replication direct snapshot",
            ["sqlalchemy>=2.0"],
            "SQLAlchemy 1.4 reads the live tables instead of the snapshot.",
        )


def pin_session_options(engine: Engine) -> Engine:
    """Makes `engine` format values like the replication connection, see `SESSION_OPTIONS`."""
    sa.event.listen(engine, "do_connect", _add_session_options)
    return engine


def _add_session_options(
    dialect: Any, connection_record: Any, cargs: Any, cparams: Dict[str, Any]
) -> None:
    cparams["options"] = add_session_options(cparams.get("options"))


def _register_interval_as_text(dbapi_connection: Any, connection_record: Any) -> None:
    register_type(_INTERVAL_AS_TEXT, dbapi_connection)
    register_type(_INTERVAL_ARRAY_AS_TEXT, dbapi_connection)


class ReplicationSnapshot(List[DltResource]):
    """Resources reading tables in the snapshot exported when the replication slot was created.

    The snapshot contains exactly the changes committed before the slot starts. It exists while
    the connection that created the slot is open and only in this process, so the resources must
    be extracted right after `init_replication`. The connection is closed when every resource
    read its table, with `close()` or when the `with` block ends.

    To read the snapshot with your own `sql_database` or `sql_table`, pass them
    `engine_adapter_callback`, `type_adapter_callback` and `table_adapter_callback` within the
    `with` block. With an arrow backend (`pyarrow`, `pandas`, `connectorx`) replicate with
    `replication_resource(backend="pyarrow")`, with `sqlalchemy` with the `python` backend.
    """

    def __init__(
        self,
        rep_conn: LogicalReplicationConnection,
        snapshot_name: str,
        reflect_not_null: bool = False,
    ) -> None:
        super().__init__()
        self.snapshot_name = snapshot_name
        self.type_adapter_callback = snapshot_type_adapter
        self.table_adapter_callback: Optional[Callable[[sa.Table], sa.Table]] = (
            None if reflect_not_null else remove_not_null_adapter
        )
        self._unread_tables = 0
        self._finalizer = weakref.finalize(self, rep_conn.close)

    @property
    def closed(self) -> bool:
        return not self._finalizer.alive

    def engine_adapter_callback(self, engine: Engine) -> Engine:
        """Makes every transaction of `engine` read the snapshot."""
        pin_session_options(engine)
        sa.event.listen(engine, "connect", _register_interval_as_text)
        sa.event.listen(engine, "begin", self._set_snapshot)
        return engine

    def table_loader_class(self) -> Type[TableLoader]:
        """Returns a `sql_table` loader that closes the snapshot when the last table is read."""
        self._unread_tables += 1
        snapshot = self

        class SnapshotTableLoader(TableLoader):
            def load_rows(
                self, backend_kwargs: Dict[str, Any] = None
            ) -> Iterator[TDataItem]:
                try:
                    yield from super().load_rows(backend_kwargs)
                finally:
                    snapshot._table_read()

        return SnapshotTableLoader

    def close(self) -> None:
        self._finalizer()

    def __enter__(self) -> "ReplicationSnapshot":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _set_snapshot(self, conn: Connection) -> None:
        # a plain cursor: sql_table streams results with server side cursors
        cur = conn.connection.dbapi_connection.cursor()
        try:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SET TRANSACTION SNAPSHOT %s", (self.snapshot_name,))
        finally:
            cur.close()

    def _table_read(self) -> None:
        self._unread_tables -= 1
        if self._unread_tables <= 0:
            self.close()
