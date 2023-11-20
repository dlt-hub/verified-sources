import pytest
import os
from typing import List, Optional

import dlt
from dlt.common.utils import uniq_id
from dlt.sources import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from sources.sql_database import sql_database, sql_table

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
from tests.sql_database.sql_source import SQLAlchemySourceDB


def make_pipeline(destination_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="sql_database",
        destination=destination_name,
        dataset_name="test_sql_pipeline_" + uniq_id(),
        full_refresh=False,
    )


def assert_row_counts(
    pipeline: dlt.Pipeline,
    sql_source_db: SQLAlchemySourceDB,
    tables: Optional[List[str]] = None,
) -> None:
    with pipeline.sql_client() as c:
        for table in tables or sql_source_db.table_infos.keys():
            info = sql_source_db.table_infos[table]
            with c.execute_query(f"SELECT count(*) FROM {table}") as cur:
                row = cur.fetchone()
                assert row[0] == info["row_count"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_schema_loads_all_tables(
    sql_source_db: SQLAlchemySourceDB, destination_name: str
) -> None:
    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(
        sql_database(credentials=sql_source_db.credentials, schema=sql_source_db.schema)
    )
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_table_names(
    sql_source_db: SQLAlchemySourceDB, destination_name: str
) -> None:
    pipeline = make_pipeline(destination_name)
    tables = ["chat_channel", "chat_message"]
    load_info = pipeline.run(
        sql_database(
            credentials=sql_source_db.credentials,
            schema=sql_source_db.schema,
            table_names=tables,
        )
    )
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, tables)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_table_incremental(
    sql_source_db: SQLAlchemySourceDB, destination_name: str
) -> None:
    """Run pipeline twice. Insert more rows after first run
    and ensure only those rows are stored after the second run.
    """
    os.environ[
        "SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCREMENTAL__CURSOR_PATH"
    ] = "updated_at"

    pipeline = make_pipeline(destination_name)
    tables = ["chat_message"]

    def make_source():  # type: ignore
        return sql_database(
            credentials=sql_source_db.credentials,
            schema=sql_source_db.schema,
            table_names=tables,
        )

    load_info = pipeline.run(make_source())
    assert_load_info(load_info)
    sql_source_db.fake_messages(n=100)
    load_info = pipeline.run(make_source())
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, tables)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_mysql_data_load(destination_name: str) -> None:
    # reflect a database
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )
    database = sql_database(credentials)
    assert "family" in database.resources

    # load a single table
    family_table = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
    )

    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(family_table, write_disposition="merge")
    assert_load_info(load_info)
    counts_1 = load_table_counts(pipeline, "family")

    # load again also with merge
    family_table = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
    )
    load_info = pipeline.run(family_table, write_disposition="merge")
    assert_load_info(load_info)
    counts_2 = load_table_counts(pipeline, "family")
    # no duplicates
    assert counts_1 == counts_2


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_table_resource_loads_data(
    sql_source_db: SQLAlchemySourceDB, destination_name: str
) -> None:
    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
            )
        ]

    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_table_resource__incremental(
    sql_source_db: SQLAlchemySourceDB, destination_name: str
) -> None:
    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                incremental=dlt.sources.incremental("updated_at"),
            )
        ]

    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)
    sql_source_db.fake_messages(n=100)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_table_resource__incremental_initial_value(
    sql_source_db: SQLAlchemySourceDB, destination_name: str
) -> None:
    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                incremental=dlt.sources.incremental(
                    "updated_at",
                    sql_source_db.table_infos["chat_message"]["created_at"].start_value,
                ),
            )
        ]

    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


def test_incremental_composite_primary_key_from_table(
    sql_source_db: SQLAlchemySourceDB,
) -> None:
    resource = sql_table(
        credentials=sql_source_db.credentials,
        table="has_composite_key",
        schema=sql_source_db.schema,
    )

    assert resource.incremental.primary_key == ["a", "b", "c"]
