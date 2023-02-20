import pytest

import dlt

from pipelines.sql_database import sql_database

from tests.utils import ALL_DESTINATIONS, assert_load_info
from tests.sql_source import SQLAlchemySourceDB
from pipelines.sql_database import sql_database


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_schema_loads_all_tables(sql_source_db: SQLAlchemySourceDB, destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name='sql_database',
        destination=destination_name,
        dataset_name='my_sql_data',
        full_refresh=False
    )
    load_info = pipeline.run(sql_database(sql_source_db.database_url, schema=sql_source_db.schema))
    assert_load_info(load_info)

    with pipeline.sql_client() as c:
        for table, info in sql_source_db.table_infos.items():
            with c.execute_query(f"SELECT count(*) FROM {table}") as cur:
                row = cur.fetchone()
                assert row[0] == info['row_count']
