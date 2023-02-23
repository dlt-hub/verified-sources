import pytest

import dlt
from dlt.common.configuration.specs import ConnectionStringCredentials

from pipelines.sql_database import sql_database

from tests.utils import ALL_DESTINATIONS, assert_load_info
from tests.sql_database.sql_source import SQLAlchemySourceDB


@pytest.fixture(scope='module')
def sql_source_db(request: pytest.FixtureRequest):
    # TODO: parametrize the fixture so it takes the credentials for all destinations
    credentials = dlt.secrets.get('destination.postgres.credentials', expected_type=ConnectionStringCredentials)
    db = SQLAlchemySourceDB(credentials)
    db.create_schema()
    try:
        db.create_tables()
        db.insert_data()
        yield db
    finally:
        db.drop_schema()


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_schema_loads_all_tables(sql_source_db: SQLAlchemySourceDB, destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name='sql_database',
        destination=destination_name,
        dataset_name='my_sql_data',
        full_refresh=False
    )
    load_info = pipeline.run(sql_database(credentials=sql_source_db.credentials, schema=sql_source_db.schema))
    assert_load_info(load_info)

    with pipeline.sql_client() as c:
        for table, info in sql_source_db.table_infos.items():
            with c.execute_query(f"SELECT count(*) FROM {table}") as cur:
                row = cur.fetchone()
                assert row[0] == info['row_count']
