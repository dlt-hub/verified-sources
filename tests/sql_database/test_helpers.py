import pytest

import dlt

from sources.sql_database.helpers import TableLoader, TableBackend
from sources.sql_database.schema_types import table_to_columns

from tests.sql_database.sql_source import SQLAlchemySourceDB


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_cursor_or_unique_column_not_in_table(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    table = sql_source_db.get_table("chat_message")

    with pytest.raises(KeyError):
        TableLoader(
            sql_source_db.engine,
            backend,
            table,
            table_to_columns(table),
            incremental=dlt.sources.incremental("not_a_column"),
        )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_max(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """Verify query is generated according to incremental settings"""

    class MockIncremental:
        last_value = dlt.common.pendulum.now()
        last_value_func = max
        cursor_path = "created_at"
        row_order = "asc"
        end_value = None

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),
    )

    query = loader.make_query()
    expected = (
        table.select()
        .order_by(table.c.created_at.asc())
        .where(table.c.created_at >= MockIncremental.last_value)
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_min(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    class MockIncremental:
        last_value = dlt.common.pendulum.now()
        last_value_func = min
        cursor_path = "created_at"
        row_order = "desc"
        end_value = None

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),
    )

    query = loader.make_query()
    print(query)
    expected = (
        table.select()
        .order_by(table.c.created_at.desc())
        .where(table.c.created_at <= MockIncremental.last_value)
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_end_value(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    now = dlt.common.pendulum.now()

    class MockIncremental:
        last_value = now
        last_value_func = min
        cursor_path = "created_at"
        end_value = now.add(hours=1)
        row_order = None

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),
    )

    query = loader.make_query()
    print(query)
    expected = (
        table.select()
        .where(table.c.created_at <= MockIncremental.last_value)
        .where(table.c.created_at > MockIncremental.end_value)
    )

    assert query.compare(expected)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_make_query_incremental_any_fun(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    class MockIncremental:
        last_value = dlt.common.pendulum.now()
        last_value_func = lambda x: x[-1]
        cursor_path = "created_at"
        row_order = "asc"
        end_value = dlt.common.pendulum.now()

    table = sql_source_db.get_table("chat_message")
    loader = TableLoader(
        sql_source_db.engine,
        backend,
        table,
        table_to_columns(table),
        incremental=MockIncremental(),
    )

    query = loader.make_query()
    expected = table.select()

    assert query.compare(expected)
