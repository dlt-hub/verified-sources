import pytest
import os
from typing import List, Optional, Set
import humanize

import dlt
from dlt.common.utils import uniq_id
from dlt.common.schema.typing import TTableSchemaColumns
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
    print(
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )
    )
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_schema_loads_all_tables_parallel(
    sql_source_db: SQLAlchemySourceDB, destination_name: str
) -> None:
    pipeline = make_pipeline(destination_name)
    source = sql_database(
        credentials=sql_source_db.credentials, schema=sql_source_db.schema
    ).parallelize()
    load_info = pipeline.run(source)
    print(
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )
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
def test_load_sql_table_resource_incremental(
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
def test_load_sql_table_resource_incremental_initial_value(
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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_sql_table_resource_incremental_end_value(
    sql_source_db: SQLAlchemySourceDB, destination_name: str
) -> None:
    start_id = sql_source_db.table_infos["chat_message"]["ids"][0]
    end_id = sql_source_db.table_infos["chat_message"]["ids"][-1] // 2

    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                incremental=dlt.sources.incremental(
                    "id", initial_value=start_id, end_value=end_id, row_order="asc"
                ),
            )
        ]

    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)
    # half of the records loaded -1 record. end values is non inclusive
    assert (
        pipeline.last_trace.last_normalize_info.row_counts["chat_message"]
        == end_id - start_id
    )


@pytest.mark.parametrize("defer_table_reflect", (False, True))
def test_load_sql_table_resource_select_columns(
    sql_source_db: SQLAlchemySourceDB, defer_table_reflect: bool
) -> None:
    # get chat messages with content column removed
    chat_messages = sql_table(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        table="chat_message",
        defer_table_reflect=defer_table_reflect,
        table_adapter_callback=lambda table: table._columns.remove(
            table.columns["content"]
        ),
    )
    pipeline = make_pipeline("duckdb")
    load_info = pipeline.run(chat_messages)
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db, ["chat_message"])
    assert "content" not in pipeline.default_schema.tables["chat_message"]["columns"]


@pytest.mark.parametrize("defer_table_reflect", (False, True))
def test_load_sql_table_source_select_columns(
    sql_source_db: SQLAlchemySourceDB, defer_table_reflect: bool
) -> None:
    mod_tables: Set[str] = set()

    def adapt(table) -> None:
        mod_tables.add(table)
        if table.name == "chat_message":
            table._columns.remove(table.columns["content"])

    # get chat messages with content column removed
    all_tables = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        defer_table_reflect=defer_table_reflect,
        table_names=sql_source_db.table_infos.keys() if defer_table_reflect else None,
        table_adapter_callback=adapt,
    )
    pipeline = make_pipeline("duckdb")
    load_info = pipeline.run(all_tables)
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db)
    assert "content" not in pipeline.default_schema.tables["chat_message"]["columns"]


def test_detect_precision_hints(sql_source_db: SQLAlchemySourceDB) -> None:
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        detect_precision_hints=True,
    )

    pipeline = make_pipeline("duckdb")

    pipeline.extract(source)
    pipeline.normalize()

    schema = pipeline.default_schema
    table = schema.tables["has_precision"]
    assert_precision_columns(table["columns"])


def test_incremental_composite_primary_key_from_table(
    sql_source_db: SQLAlchemySourceDB,
) -> None:
    resource = sql_table(
        credentials=sql_source_db.credentials,
        table="has_composite_key",
        schema=sql_source_db.schema,
    )

    assert resource.incremental.primary_key == ["a", "b", "c"]


@pytest.mark.parametrize("upfront_incremental", (True, False))
def test_set_primary_key_deferred_incremental(
    sql_source_db: SQLAlchemySourceDB,
    upfront_incremental: bool,
) -> None:
    # this tests dynamically adds primary key to resource and as consequence to incremental
    updated_at = dlt.sources.incremental("updated_at")
    resource = sql_table(
        credentials=sql_source_db.credentials,
        table="chat_message",
        schema=sql_source_db.schema,
        defer_table_reflect=True,
        incremental=updated_at if upfront_incremental else None,
    )

    resource.apply_hints(incremental=None if upfront_incremental else updated_at)

    # nothing set for deferred reflect
    assert resource.incremental.primary_key == []

    def _assert_incremental(item):
        # for all the items, all keys must be present
        _r = dlt.current.source().resources[dlt.current.resource_name()]
        # assert _r.incremental._incremental is updated_at
        if len(item) == 0:
            # not yet propagated
            assert _r.incremental.primary_key == []
        else:
            assert _r.incremental.primary_key == ["id"]
        assert _r.incremental._incremental.primary_key == ["id"]
        assert _r.incremental._incremental._transformers["json"].primary_key == ["id"]
        return item

    pipeline = make_pipeline("duckdb")
    # must evaluate resource for primary key to be set
    pipeline.extract(resource.add_step(_assert_incremental))

    assert resource.incremental.primary_key == ["id"]
    assert resource.incremental._incremental.primary_key == ["id"]
    assert resource.incremental._incremental._transformers["json"].primary_key == ["id"]


def test_deferred_reflect_in_source(sql_source_db: SQLAlchemySourceDB) -> None:
    source = sql_database(
        credentials=sql_source_db.credentials,
        table_names=["has_precision", "chat_message"],
        schema=sql_source_db.schema,
        detect_precision_hints=True,
        defer_table_reflect=True,
    )

    # no columns in both tables
    assert source.has_precision.columns == {}
    assert source.chat_message.columns == {}

    pipeline = make_pipeline("duckdb")
    pipeline.extract(source)
    assert_precision_columns(source.has_precision.columns)
    assert len(source.chat_message.columns) > 0
    assert (
        source.chat_message.compute_table_schema()["columns"]["id"]["primary_key"]
        is True
    )


def test_deferred_reflect_in_resource(sql_source_db: SQLAlchemySourceDB) -> None:
    table = sql_table(
        credentials=sql_source_db.credentials,
        table="has_precision",
        schema=sql_source_db.schema,
        detect_precision_hints=True,
        defer_table_reflect=True,
    )

    # no columns in both tables
    assert table.columns == {}

    pipeline = make_pipeline("duckdb")
    pipeline.extract(table)
    assert_precision_columns(
        pipeline.default_schema.get_table("has_precision")["columns"]
    )


def assert_precision_columns(columns: TTableSchemaColumns) -> None:
    assert columns["bigint_col"] == {
        "data_type": "bigint",
        "precision": 64,
        "name": "bigint_col",
    }
    assert columns["int_col"] == {
        "data_type": "bigint",
        "precision": 32,
        "name": "int_col",
    }
    assert columns["smallint_col"] == {
        "data_type": "bigint",
        "precision": 16,
        "name": "smallint_col",
    }
    assert columns["numeric_col"] == {
        "data_type": "decimal",
        "precision": 10,
        "scale": 2,
        "name": "numeric_col",
    }
    assert columns["numeric_default_col"] == {
        "data_type": "decimal",
        "name": "numeric_default_col",
    }
    assert columns["string_col"] == {
        "data_type": "text",
        "precision": 10,
        "name": "string_col",
    }
    assert columns["string_default_col"] == {
        "data_type": "text",
        "name": "string_default_col",
    }
