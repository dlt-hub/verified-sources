import pytest
import os

import dlt
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.utils import uniq_id

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
        dataset_name='test_sql_pipeline_' + uniq_id(),
        full_refresh=False
    )
    load_info = pipeline.run(sql_database(credentials=sql_source_db.credentials, schema=sql_source_db.schema))
    assert_load_info(load_info)

    with pipeline.sql_client() as c:
        for table, info in sql_source_db.table_infos.items():
            with c.execute_query(f"SELECT count(*) FROM {table}") as cur:
                row = cur.fetchone()
                assert row[0] == info['row_count']


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_sql_table_names(sql_source_db: SQLAlchemySourceDB, destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name='sql_database',
        destination=destination_name,
        dataset_name='test_sql_pipeline_' + uniq_id(),
        full_refresh=False
    )
    tables = ['chat_channel', 'chat_message']
    load_info = pipeline.run(
        sql_database(
            credentials = sql_source_db.credentials,
            schema=sql_source_db.schema,
            table_names=tables
        )
    )
    assert_load_info(load_info)

    with pipeline.sql_client() as c:
        for table in tables:
            info = sql_source_db.table_infos[table]
            with c.execute_query(f"SELECT count(*) FROM {table}") as cur:
                row = cur.fetchone()
                assert row[0] == info['row_count']


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_sql_table_incremental(sql_source_db: SQLAlchemySourceDB, destination_name: str) -> None:
    """Run pipeline twice. Insert more rows after first run
    and ensure only those rows are stored after the second run.
    """
    os.environ['SOURCES__SQL_DATABASE__CHAT_MESSAGE__CURSOR_COLUMN'] = 'updated_at'
    os.environ['SOURCES__SQL_DATABASE__CHAT_MESSAGE__UNIQUE_COLUMN'] = 'id'

    pipeline = dlt.pipeline(
        pipeline_name='sql_database',
        destination=destination_name,
        dataset_name='test_sql_pipeline_' + uniq_id(),
        full_refresh=False
    )
    tables = ['chat_message']

    def make_source():  # type: ignore
        return sql_database(
            credentials = sql_source_db.credentials,
            schema=sql_source_db.schema,
            table_names=tables
        )

    load_info = pipeline.run(
        make_source()
    )
    assert_load_info(load_info)
    sql_source_db.fake_messages(n=100)
    load_info = pipeline.run(
        make_source()
    )
    assert_load_info(load_info)

    with pipeline.sql_client() as c:
        for table in tables:
            info = sql_source_db.table_infos[table]
            with c.execute_query(f"SELECT count(*) FROM {table}") as cur:
                row = cur.fetchone()
                assert row[0] == info['row_count']
