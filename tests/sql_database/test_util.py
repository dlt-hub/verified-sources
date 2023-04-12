import pytest

import dlt

from pipelines.sql_database.util import TableLoader
from tests.sql_database.sql_source import SQLAlchemySourceDB


class TestTableLoader:
    def test_cursor_or_unique_column_not_in_table(self, sql_source_db: SQLAlchemySourceDB) -> None:
        table = sql_source_db.get_table('chat_message')

        with pytest.raises(KeyError):
            TableLoader(
                sql_source_db.engine, table, incremental=dlt.sources.incremental('not_a_column')
            )

    def test_make_query_incremental_max(self, sql_source_db: SQLAlchemySourceDB) -> None:
        """Verify query is generated according to incremental settings"""
        class MockIncremental:
            last_value = dlt.common.pendulum.now()
            last_value_func = max
            cursor_path = 'created_at'

        table = sql_source_db.get_table('chat_message')
        loader = TableLoader(sql_source_db.engine, table, incremental=MockIncremental())

        query = loader.make_query()
        expected = table.select().order_by(table.c.created_at.asc()).where(table.c.created_at >= MockIncremental.last_value)

        assert query.compare(expected)

    def test_make_query_incremental_min(self, sql_source_db: SQLAlchemySourceDB) -> None:
        class MockIncremental:
            last_value = dlt.common.pendulum.now()
            last_value_func = min
            cursor_path = 'created_at'

        table = sql_source_db.get_table('chat_message')
        loader = TableLoader(sql_source_db.engine, table, incremental=MockIncremental())

        query = loader.make_query()
        expected = table.select().order_by(table.c.created_at.desc()).where(table.c.created_at <= MockIncremental.last_value)

        assert query.compare(expected)

    def test_make_query_incremental_any_fun(self, sql_source_db: SQLAlchemySourceDB) -> None:
        class MockIncremental:
            last_value = dlt.common.pendulum.now()
            last_value_func = lambda x: x[-1]
            cursor_path = 'created_at'

        table = sql_source_db.get_table('chat_message')
        loader = TableLoader(sql_source_db.engine, table, incremental=MockIncremental())

        query = loader.make_query()
        expected = table.select()

        assert query.compare(expected)
