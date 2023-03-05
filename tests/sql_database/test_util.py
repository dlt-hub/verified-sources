import pytest

from pipelines.sql_database.util import Cursor, TableLoader
from tests.sql_database.sql_source import SQLAlchemySourceDB


class TestTableLoader:
    def test_cursor_or_unique_column_not_in_table(self, sql_source_db: SQLAlchemySourceDB) -> None:
        table = sql_source_db.get_table('chat_message')

        with pytest.raises(KeyError):
            TableLoader(
                sql_source_db.engine, table, 'not_a_column', 'id'
            )

        with pytest.raises(KeyError):
            TableLoader(
                sql_source_db.engine, table, 'updated_at', 'not_a_column'
            )
