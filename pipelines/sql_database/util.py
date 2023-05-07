from typing import cast, TypedDict, Any, List, Optional, Mapping, Iterator, Dict, Union, Sequence
import operator

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from sqlalchemy import Table, tuple_, create_engine
from sqlalchemy.engine import Engine, Row
from sqlalchemy.sql import Select


class TableLoader:
    def __init__(
        self,
        engine: Engine,
        table: Table,
        chunk_size: int = 1000,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> None:
        self.engine = engine
        self.table = table
        self.chunk_size = chunk_size
        self.incremental = incremental
        if incremental:
            try:
                self.cursor_column = table.c[incremental.cursor_path]
            except KeyError as e:
                raise KeyError(
                    f"Cursor column '{incremental.cursor_path}' does not exist in table '{table.name}'"
                ) from e
            self.last_value = incremental.last_value
        else:
            self.cursor_column = None
            self.last_value = None

    def make_query(self) -> Select[Any]:
        table = self.table
        query = table.select()
        if not self.incremental:
            return query
        last_value_func = self.incremental.last_value_func
        if last_value_func is max:  # Query ordered and filtered according to last_value function
            order_by = self.cursor_column.asc()
            filter_op = operator.ge
        elif last_value_func is min:
            order_by = self.cursor_column.desc()
            filter_op = operator.le
        else:  # Custom last_value, load everything and let incremental handle filtering
            return query
        query = query.order_by(order_by)
        if self.last_value is None:
            return cast(Select[Any], query)  # TODO: typing in sqlalchemy 2
        return cast(Select[Any], query.where(filter_op(self.cursor_column, self.last_value)))

    def load_rows(self) -> Iterator[List[Dict[str, Any]]]:
        query = self.make_query()

        with self.engine.connect() as conn:
            result = conn.execution_options(yield_per=self.chunk_size).execute(query)
            for partition in result.partitions():
                yield [dict(row._mapping) for row in partition]


def table_rows(
    engine: Engine,
    table: Table,
    chunk_size: int = 1000,
    incremental: Optional[dlt.sources.incremental[Any]] = None
) -> Iterator[List[Dict[str, Any]]]:
    """Yields rows from the given database table.
    :param table: The table name to load data from
    :param incremental: Option to enable incremental loading for the table. E.g. `incremental=dlt.source.incremental('updated_at', initial_value=pendulum.parse('2022-01-01T00:00:00Z'))`
    :param chunk_size: How many rows to read from db at a time
    """
    loader = TableLoader(engine, table, incremental=incremental, chunk_size=chunk_size)
    yield from loader.load_rows()


def engine_from_credentials(credentials: Union[ConnectionStringCredentials, Engine, str]) -> Engine:
    if isinstance(credentials, Engine):
        return credentials
    if isinstance(credentials, ConnectionStringCredentials):
        credentials = credentials.to_native_representation()
    return create_engine(credentials)


def get_primary_key(table: Table) -> List[str]:
    return [c.name for c in table.primary_key]


__source_name__ = 'sql_database'
