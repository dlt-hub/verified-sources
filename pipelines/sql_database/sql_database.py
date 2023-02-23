from typing import List, Iterator, Dict, Any, Optional, Tuple, Union, Mapping, cast, TypedDict
from collections import defaultdict

import dlt
from dlt.extract.source import DltResource
from dlt.common import json

from sqlalchemy import create_engine, MetaData, Table, Column, tuple_
from sqlalchemy.sql import Select
from sqlalchemy.engine import Engine, Connection


class CursorState(TypedDict):
    last_value: Any
    loaded_ids: List[Any]


def _get_cursor_state(table_name: str, cursor_column: str) -> CursorState:
    """Get the last loaded value of the table's cursor column from state.
    """
    states = dlt.state().setdefault('incremental', {})
    table_state = states.setdefault(table_name, {})
    cursor_state = table_state.setdefault(cursor_column, {'last_value': None, 'loaded_ids': []})
    return cast(CursorState, cursor_state)


def _set_last_value(table_name: str, cursor_column: str, value: Any, loaded_id: Any) -> None:
    cursor_state = _get_cursor_state(table_name, cursor_column)
    current_val = cursor_state['last_value']
    if current_val != value:
        cursor_state['last_value'] = value
        cursor_state['loaded_ids'] = []
    if loaded_id is not None:
        cursor_state['loaded_ids'].append(loaded_id)


def _make_query(table: Table, cursor_column: Optional[str], unique_column: Optional[str]) -> Tuple[Select[Any], Optional[Column[Any]], Optional[Column[Any]]]:
    """Make the SQL query object to select from table.
    Returns a tuple with the query and optionally the cursor column as `sqlalchemy.Column` object
    """
    query = table.select()
    if not cursor_column:
        return query, None, None
    cursor_col = table.c.get(cursor_column)
    if cursor_col is None:
        # Cursor column doesn't exist in table
        # TODO: Should this print a warning or exception?
        return query, None, None
    query = query.order_by(cursor_col)
    cursor_state = _get_cursor_state(table.name, cursor_column)
    last_value = cursor_state['last_value']
    loaded_ids = cursor_state['loaded_ids']
    unique_col = table.c.get(unique_column)
    if last_value is None:
        return query, cursor_col, unique_col
    query = query.where(cursor_col >= last_value)
    if not not loaded_ids and unique_col is not None:
        query = query.where(tuple_(cursor_col, unique_col).notin_(
            [(last_value, lid) for lid in loaded_ids]
        ))
    return query, cursor_col, unique_col


def table_rows(
    engine: Engine,
    table: Table,
    cursor_column: Optional[str] = None,
    unique_column: Optional[str] = None
) -> Iterator[Dict[str, Any]]:
    """Yields rows from the given database table.

    :param engine: An `sqlalchemy.engine.Engine` instance configured for the database
    :param table: The table to load data from
    :param cursor_column: Optional column name to use as cursor for resumeable loading
    :param unique_column: Optional column that uniquely identifies a row in the table for resumeable loading
    """
    query, cursor, unique = _make_query(table, cursor_column, unique_column)

    conn: Connection
    with engine.connect() as conn:
        result = conn.execution_options(yield_per=1000).execute(query)
        for partition in result.partitions():
            for row in partition:
                if cursor is not None:
                    _set_last_value(
                        table.name, cursor_column, row._mapping[cursor], row._mapping.get(unique.name) if unique is not None else None
                    )
                yield dict(row._mapping)


@dlt.source
def sql_database(
    database_url: str = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    table_names: Optional[List[str]] = dlt.config.value,
    write_disposition: str = 'replace',
) -> List[DltResource]:
    """A dlt source which loads data from an SQL database using SQLAlchemy.
    Resources are automatically created for each table in the schema or from the given list of tables.

    :param database_url: An SQLAlchemy database connection string (e.g. `postgresql://user:password@localhost/my_database`)
    :param schema: Name of the database schema to load (if different from default)
    :param table_names: A list of table names to load. By default all defaults in the schema are loaded.

    :return: A list of dlt resources for each table to be loaded
    """
    engine = create_engine(database_url)
    engine.execution_options(stream_results=True)
    metadata = MetaData(
        schema=schema,
    )
    metadata.reflect(bind=engine)

    tables = [
        table
        for table in metadata.tables.values()
        if not table_names or table.name in table_names
    ]
    return [
        dlt.resource(table_rows, name=table.name)(
            engine, table
        )
        for table in tables
    ]
