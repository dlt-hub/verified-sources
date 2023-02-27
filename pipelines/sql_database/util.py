from typing import cast, TypedDict, Any, List, Optional, Mapping, Iterator, Dict

from sqlalchemy import Table, tuple_
from sqlalchemy.engine import Result, Engine
from sqlalchemy.sql import Select

import dlt


class CursorState(TypedDict):
    last_value: Any
    loaded_ids: List[Any]



class Cursor:
    def __init__(self, table_name: str, cursor_column: str, unique_column: Optional[str]) -> None:
        self.table_name = table_name
        self.cursor_column = cursor_column
        self.unique_column = unique_column

    def get_state(self) -> CursorState:
        states = dlt.state().setdefault('incremental', {})
        table_state = states.setdefault(self.table_name, {})
        cursor_state = table_state.setdefault(self.cursor_column, {'last_value': None, 'loaded_ids': []})
        return cast(CursorState, cursor_state)

    def update(self, cursor_value: Any, loaded_id: Any) -> None:
        state = self.get_state()
        current_val = state['last_value']
        if current_val != cursor_value:
            state['last_value'] = cursor_value
            state['loaded_ids'] = []
        if loaded_id is not None:
            state['loaded_ids'].append(loaded_id)


class TableLoader:
    def __init__(
        self,
        engine: Engine,
        table: Table,
        cursor_column: Optional[str] = None,
        unique_column: Optional[str] = None
    ) -> None:
        self.engine = engine
        self.table = table
        self.cursor = (
            Cursor(table.name, cursor_column, unique_column)
            if cursor_column is not None else None
        )
        self.cursor_column = table.c.get(cursor_column)
        self.unique_column = table.c.get(unique_column)
        self.table_has_cursor = self.cursor_column is not None
        self.table_has_unique = self.unique_column is not None

    def make_query(self) -> Select[Any]:
        table = self.table
        query = table.select()
        if not self.cursor:
            return query
        cursor_col = self.cursor_column
        if cursor_col is None:
            # Cursor column doesn't exist in table
            # TODO: Should this print a warning or exception?
            return query
        query = query.order_by(cursor_col)
        cursor_state = self.cursor.get_state()
        last_value = cursor_state['last_value']
        loaded_ids = cursor_state['loaded_ids']
        unique_col = self.unique_column
        if last_value is None:
            return cast(Select[Any], query)  # TODO: typing in sqlalchemy 2
        query = query.where(cursor_col >= last_value)
        if not not loaded_ids and unique_col is not None:
            query = query.where(tuple_(cursor_col, unique_col).notin_(
                [(last_value, lid) for lid in loaded_ids]
            ))
        return query

    def _update_cursor(self, row: Mapping[str, Any]) -> None:
        cursor = self.cursor
        if not cursor:
            return
        if not self.table_has_cursor:
            return
        self.cursor.update(
            row[cursor.cursor_column],
            row[cursor.unique_column] if self.table_has_unique else None
        )

    def load_rows(self) -> Iterator[Dict[str, Any]]:
        query = self.make_query()

        with self.engine.connect() as conn:
            result = conn.execution_options(yield_per=1000).execute(query)
            for partition in result.partitions():
                for row in partition:
                    row_mapping = row._mapping
                    self._update_cursor(row_mapping)
                    yield dict(row_mapping)


def table_rows(
    engine: Engine,
    table: Table,
    cursor_column: Optional[str] = None,
    unique_column: Optional[str] = None
) -> Iterator[Dict[str, Any]]:
    """Yields rows from the given database table.
    :param table: The table name to load data from
    :param cursor_column: Optional column name to use as cursor for resumeable loading
    :param unique_column: Optional column that uniquely identifies a row in the table for resumeable loading
    """
    loader = TableLoader(engine, table, cursor_column, unique_column)
    yield from loader.load_rows()
