from copy import deepcopy
import pytest
import os
from typing import Any, List, Optional, Set
import humanize
import sqlalchemy as sa

import dlt
from dlt.common.utils import uniq_id
from dlt.common.schema.typing import TTableSchemaColumns, TColumnSchema, TSortOrder

from dlt.sources import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from sources.sql_database import sql_database, sql_table, TableBackend
from sources.sql_database.helpers import unwrap_json_connector_x

from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    data_item_length,
    load_table_counts,
    load_tables_to_dicts,
    assert_schema_on_data,
)
from tests.sql_database.sql_source import SQLAlchemySourceDB


def make_pipeline(destination_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="sql_database",
        destination=destination_name,
        dataset_name="test_sql_pipeline_" + uniq_id(),
        full_refresh=False,
    )


def test_pass_engine_credentials(sql_source_db: SQLAlchemySourceDB) -> None:
    # verify database
    database = sql_database(
        sql_source_db.engine, schema=sql_source_db.schema, table_names=["chat_message"]
    )
    assert len(list(database)) == sql_source_db.table_infos["chat_message"]["row_count"]

    # verify table
    table = sql_table(
        sql_source_db.engine, table="chat_message", schema=sql_source_db.schema
    )
    assert len(list(table)) == sql_source_db.table_infos["chat_message"]["row_count"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_schema_loads_all_tables(
    sql_source_db: SQLAlchemySourceDB, destination_name: str, backend: TableBackend
) -> None:
    pipeline = make_pipeline(destination_name)
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        backend=backend,
    )

    assert (
        "chat_message_view" not in source.resources
    )  # Views are not reflected by default

    load_info = pipeline.run(source)
    print(
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )
    )
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_schema_loads_all_tables_parallel(
    sql_source_db: SQLAlchemySourceDB, destination_name: str, backend: TableBackend
) -> None:
    pipeline = make_pipeline(destination_name)
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        backend=backend,
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
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_table_names(
    sql_source_db: SQLAlchemySourceDB, destination_name: str, backend: TableBackend
) -> None:
    pipeline = make_pipeline(destination_name)
    tables = ["chat_channel", "chat_message"]
    load_info = pipeline.run(
        sql_database(
            credentials=sql_source_db.credentials,
            schema=sql_source_db.schema,
            table_names=tables,
            backend=backend,
        )
    )
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, tables)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_table_incremental(
    sql_source_db: SQLAlchemySourceDB, destination_name: str, backend: TableBackend
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
            backend=backend,
        )

    load_info = pipeline.run(make_source())
    assert_load_info(load_info)
    sql_source_db.fake_messages(n=100)
    load_info = pipeline.run(make_source())
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, tables)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_mysql_data_load(destination_name: str, backend: TableBackend) -> None:
    # reflect a database
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )
    database = sql_database(credentials)
    assert "family" in database.resources

    if backend == "connectorx":
        # connector-x has different connection string format
        backend_kwargs = {
            "conn": "mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
        }
    else:
        backend_kwargs = {}

    # no longer needed: asdecimal used to infer decimal or not
    # def _double_as_decimal_adapter(table: sa.Table) -> sa.Table:
    #     for column in table.columns.values():
    #         if isinstance(column.type, sa.Double):
    #             column.type.asdecimal = False

    # load a single table
    family_table = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
        backend=backend,
        backend_kwargs=backend_kwargs,
        # table_adapter_callback=_double_as_decimal_adapter,
    )

    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(family_table, write_disposition="merge")
    assert_load_info(load_info)
    counts_1 = load_table_counts(pipeline, "family")

    # load again also with merge
    family_table = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
        backend=backend,
        # we also try to remove dialect automatically
        backend_kwargs={},
        # table_adapter_callback=_double_as_decimal_adapter,
    )
    load_info = pipeline.run(family_table, write_disposition="merge")
    assert_load_info(load_info)
    counts_2 = load_table_counts(pipeline, "family")
    # no duplicates
    assert counts_1 == counts_2


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_table_resource_loads_data(
    sql_source_db: SQLAlchemySourceDB, destination_name: str, backend: TableBackend
) -> None:
    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                backend=backend,
            )
        ]

    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_load_sql_table_resource_incremental(
    sql_source_db: SQLAlchemySourceDB, destination_name: str, backend: TableBackend
) -> None:
    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                incremental=dlt.sources.incremental("updated_at"),
                backend=backend,
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
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_load_sql_table_resource_incremental_initial_value(
    sql_source_db: SQLAlchemySourceDB, destination_name: str, backend: TableBackend
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
                backend=backend,
            )
        ]

    pipeline = make_pipeline(destination_name)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
@pytest.mark.parametrize("row_order", ["asc", "desc", None])
@pytest.mark.parametrize("last_value_func", [min, max, lambda x: max(x)])
def test_load_sql_table_resource_incremental_end_value(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    row_order: TSortOrder,
    last_value_func: Any,
) -> None:
    start_id = sql_source_db.table_infos["chat_message"]["ids"][0]
    end_id = sql_source_db.table_infos["chat_message"]["ids"][-1] // 2

    if last_value_func is min:
        start_id, end_id = end_id, start_id

    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                backend=backend,
                incremental=dlt.sources.incremental(
                    "id",
                    initial_value=start_id,
                    end_value=end_id,
                    row_order=row_order,
                    last_value_func=last_value_func,
                ),
            )
        ]

    try:
        rows = list(sql_table_source())
    except Exception as exc:
        if isinstance(exc.__context__, NotImplementedError):
            pytest.skip("Test skipped due to: " + str(exc.__context__))
    # half of the records loaded -1 record. end values is non inclusive
    assert data_item_length(rows) == abs(end_id - start_id)
    # check first and last id to see if order was applied
    if backend == "sqlalchemy":
        if row_order == "asc" and last_value_func is max:
            assert rows[0]["id"] == start_id
            assert rows[-1]["id"] == end_id - 1  # non inclusive
        if row_order == "desc" and last_value_func is max:
            assert rows[0]["id"] == end_id - 1  # non inclusive
            assert rows[-1]["id"] == start_id
        if row_order == "asc" and last_value_func is min:
            assert rows[0]["id"] == start_id
            assert (
                rows[-1]["id"] == end_id + 1
            )  # non inclusive, but + 1 because last value func is min
        if row_order == "desc" and last_value_func is min:
            assert (
                rows[0]["id"] == end_id + 1
            )  # non inclusive, but + 1 because last value func is min
            assert rows[-1]["id"] == start_id


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("defer_table_reflect", (False, True))
def test_load_sql_table_resource_select_columns(
    sql_source_db: SQLAlchemySourceDB, defer_table_reflect: bool, backend: str
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
        backend=backend,
    )
    pipeline = make_pipeline("duckdb")
    load_info = pipeline.run(chat_messages)
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db, ["chat_message"])
    assert "content" not in pipeline.default_schema.tables["chat_message"]["columns"]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("defer_table_reflect", (False, True))
def test_load_sql_table_source_select_columns(
    sql_source_db: SQLAlchemySourceDB, defer_table_reflect: bool, backend: TableBackend
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
        backend=backend,
    )
    pipeline = make_pipeline("duckdb")
    load_info = pipeline.run(all_tables)
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db)
    assert "content" not in pipeline.default_schema.tables["chat_message"]["columns"]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("with_precision", [True, False])
@pytest.mark.parametrize("with_defer", [True, False])
def test_extract_without_pipeline(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    with_precision: bool,
    with_defer: bool,
) -> None:
    # make sure that we can evaluate tables without pipeline
    source = sql_database(
        credentials=sql_source_db.credentials,
        table_names=["has_precision", "app_user", "chat_message", "chat_channel"],
        schema=sql_source_db.schema,
        detect_precision_hints=with_precision,
        defer_table_reflect=with_defer,
        backend=backend,
    )
    assert len(list(source)) > 0


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize(
    "table_name,nullable", (("has_precision", False), ("has_precision_nullable", True))
)
def test_all_types_with_precision_hints(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    table_name: str,
    nullable: bool,
) -> None:
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        detect_precision_hints=True,
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")

    # add JSON unwrap for connectorx
    if backend == "connectorx":
        source.resources[table_name].add_map(unwrap_json_connector_x("json_col"))
    pipeline.extract(source)
    pipeline.normalize(loader_file_format="parquet")
    info = pipeline.load()
    assert_load_info(info)

    schema = pipeline.default_schema
    table = schema.tables[table_name]
    assert_precision_columns(table["columns"], backend, nullable)
    assert_schema_on_data(
        table, load_tables_to_dicts(pipeline, table_name)[table_name], nullable
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize(
    "table_name,nullable", (("has_precision", False), ("has_precision_nullable", True))
)
def test_all_types_no_precision_hints(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    table_name: str,
    nullable: bool,
) -> None:
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        detect_precision_hints=False,
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")

    # add JSON unwrap for connectorx
    if backend == "connectorx":
        source.resources[table_name].add_map(unwrap_json_connector_x("json_col"))
    pipeline.extract(source)
    pipeline.normalize(loader_file_format="parquet")
    pipeline.load()

    schema = pipeline.default_schema
    # print(pipeline.default_schema.to_pretty_yaml())
    table = schema.tables[table_name]
    assert_no_precision_columns(table["columns"], backend, nullable)
    assert_schema_on_data(
        table, load_tables_to_dicts(pipeline, table_name)[table_name], nullable
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_incremental_composite_primary_key_from_table(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
) -> None:
    resource = sql_table(
        credentials=sql_source_db.credentials,
        table="has_composite_key",
        schema=sql_source_db.schema,
        backend=backend,
    )

    assert resource.incremental.primary_key == ["a", "b", "c"]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("upfront_incremental", (True, False))
def test_set_primary_key_deferred_incremental(
    sql_source_db: SQLAlchemySourceDB,
    upfront_incremental: bool,
    backend: TableBackend,
) -> None:
    # this tests dynamically adds primary key to resource and as consequence to incremental
    updated_at = dlt.sources.incremental("updated_at")
    resource = sql_table(
        credentials=sql_source_db.credentials,
        table="chat_message",
        schema=sql_source_db.schema,
        defer_table_reflect=True,
        incremental=updated_at if upfront_incremental else None,
        backend=backend,
    )

    resource.apply_hints(incremental=None if upfront_incremental else updated_at)

    # nothing set for deferred reflect
    assert resource.incremental.primary_key is None

    def _assert_incremental(item):
        # for all the items, all keys must be present
        _r = dlt.current.source().resources[dlt.current.resource_name()]
        # assert _r.incremental._incremental is updated_at
        if len(item) == 0:
            # not yet propagated
            assert _r.incremental.primary_key == ["id"]
        else:
            assert _r.incremental.primary_key == ["id"]
        assert _r.incremental._incremental.primary_key == ["id"]
        assert _r.incremental._incremental._transformers["json"].primary_key == ["id"]
        assert _r.incremental._incremental._transformers["arrow"].primary_key == ["id"]
        return item

    pipeline = make_pipeline("duckdb")
    # must evaluate resource for primary key to be set
    pipeline.extract(resource.add_step(_assert_incremental))

    assert resource.incremental.primary_key == ["id"]
    assert resource.incremental._incremental.primary_key == ["id"]
    assert resource.incremental._incremental._transformers["json"].primary_key == ["id"]
    assert resource.incremental._incremental._transformers["arrow"].primary_key == [
        "id"
    ]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_deferred_reflect_in_source(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    source = sql_database(
        credentials=sql_source_db.credentials,
        table_names=["has_precision", "chat_message"],
        schema=sql_source_db.schema,
        detect_precision_hints=True,
        defer_table_reflect=True,
        backend=backend,
    )
    # add JSON unwrap for connectorx
    if backend == "connectorx":
        source.resources["has_precision"].add_map(unwrap_json_connector_x("json_col"))

    # no columns in both tables
    assert source.has_precision.columns == {}
    assert source.chat_message.columns == {}

    pipeline = make_pipeline("duckdb")
    pipeline.extract(source)
    # use insert values to convert parquet into INSERT
    pipeline.normalize(loader_file_format="insert_values")
    pipeline.load().raise_on_failed_jobs()
    precision_table = pipeline.default_schema.get_table("has_precision")
    assert_precision_columns(
        precision_table["columns"],
        backend,
        nullable=False,
    )
    assert_schema_on_data(
        precision_table,
        load_tables_to_dicts(pipeline, "has_precision")["has_precision"],
        True,
    )
    assert len(source.chat_message.columns) > 0
    assert (
        source.chat_message.compute_table_schema()["columns"]["id"]["primary_key"]
        is True
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_deferred_reflect_no_source_connect(backend: TableBackend) -> None:
    source = sql_database(
        credentials="mysql+pymysql://test@test/test",
        table_names=["has_precision", "chat_message"],
        schema="schema",
        detect_precision_hints=True,
        defer_table_reflect=True,
        backend=backend,
    )

    # no columns in both tables
    assert source.has_precision.columns == {}
    assert source.chat_message.columns == {}


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_deferred_reflect_in_resource(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    table = sql_table(
        credentials=sql_source_db.credentials,
        table="has_precision",
        schema=sql_source_db.schema,
        detect_precision_hints=True,
        defer_table_reflect=True,
        backend=backend,
    )
    # add JSON unwrap for connectorx
    if backend == "connectorx":
        table.add_map(unwrap_json_connector_x("json_col"))

    # no columns in both tables
    assert table.columns == {}

    pipeline = make_pipeline("duckdb")
    pipeline.extract(table)
    # use insert values to convert parquet into INSERT
    pipeline.normalize(loader_file_format="insert_values")
    pipeline.load().raise_on_failed_jobs()
    precision_table = pipeline.default_schema.get_table("has_precision")
    assert_precision_columns(
        precision_table["columns"],
        backend,
        nullable=False,
    )
    assert_schema_on_data(
        precision_table,
        load_tables_to_dicts(pipeline, "has_precision")["has_precision"],
        True,
    )


@pytest.mark.parametrize("backend", ["pyarrow", "pandas", "connectorx"])
def test_destination_caps_context(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    # use athena with timestamp precision == 3
    table = sql_table(
        credentials=sql_source_db.credentials,
        table="has_precision",
        schema=sql_source_db.schema,
        detect_precision_hints=True,
        defer_table_reflect=True,
        backend=backend,
    )

    # no columns in both tables
    assert table.columns == {}

    pipeline = make_pipeline("athena")
    pipeline.extract(table)
    pipeline.normalize()
    # timestamps are milliseconds
    columns = pipeline.default_schema.get_table("has_precision")["columns"]
    assert (
        columns["datetime_tz_col"]["precision"]
        == columns["datetime_ntz_col"]["precision"]
        == 3
    )
    # prevent drop
    pipeline.destination = None


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_sql_table_from_view(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """View can be extract by sql_table without any reflect flags"""
    table = sql_table(
        credentials=sql_source_db.credentials,
        table="chat_message_view",
        schema=sql_source_db.schema,
    )

    pipeline = make_pipeline("duckdb")
    pipeline.run(table)

    assert_row_counts(pipeline, sql_source_db, ["chat_message_view"])
    assert "content" in pipeline.default_schema.tables["chat_message_view"]["columns"]
    assert (
        "content"
        in load_tables_to_dicts(pipeline, "chat_message_view")["chat_message_view"][0]
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_sql_database_include_views(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """include_view flag reflects and extracts views as tables"""
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        include_views=True,
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")
    pipeline.run(source)

    assert_row_counts(pipeline, sql_source_db, include_views=True)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_sql_database_include_view_in_table_names(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """Passing a view explicitly in table_names should reflect it, regardless of include_views flag"""
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        table_names=["app_user", "chat_message_view"],
        include_views=False,
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")
    pipeline.run(source)

    assert_row_counts(pipeline, sql_source_db, ["app_user", "chat_message_view"])


def assert_row_counts(
    pipeline: dlt.Pipeline,
    sql_source_db: SQLAlchemySourceDB,
    tables: Optional[List[str]] = None,
    include_views: bool = False,
) -> None:
    with pipeline.sql_client() as c:
        if not tables:
            tables = [
                tbl_name
                for tbl_name, info in sql_source_db.table_infos.items()
                if include_views or not info["is_view"]
            ]
        for table in tables:
            info = sql_source_db.table_infos[table]
            with c.execute_query(f"SELECT count(*) FROM {table}") as cur:
                row = cur.fetchone()
                assert row[0] == info["row_count"]


def assert_precision_columns(
    columns: TTableSchemaColumns, backend: TableBackend, nullable: bool
) -> None:
    actual = list(columns.values())
    expected = NULL_PRECISION_COLUMNS if nullable else NOT_NULL_PRECISION_COLUMNS
    # always has nullability set and always has hints
    expected = deepcopy(expected)
    if backend == "sqlalchemy":
        expected = remove_timestamp_precision(expected)
        actual = remove_dlt_columns(actual)
    if backend == "pyarrow":
        expected = add_default_decimal_precision(expected)
    if backend == "pandas":
        expected = remove_timestamp_precision(expected, with_timestamps=False)
    if backend == "connectorx":
        # connector x emits 32 precision which gets merged with sql alchemy schema
        del columns["int_col"]["precision"]
    assert actual == expected


def assert_no_precision_columns(
    columns: TTableSchemaColumns, backend: TableBackend, nullable: bool
) -> None:
    actual = list(columns.values())

    # we always infer and emit nullability
    expected = deepcopy(
        NULL_NO_PRECISION_COLUMNS if nullable else NOT_NULL_NO_PRECISION_COLUMNS
    )
    if backend == "pyarrow":
        expected = deepcopy(
            NULL_PRECISION_COLUMNS if nullable else NOT_NULL_PRECISION_COLUMNS
        )
        # always has nullability set and always has hints
        # default precision is not set
        expected = remove_default_precision(expected)
        expected = add_default_decimal_precision(expected)
    elif backend == "sqlalchemy":
        # no precision, no nullability, all hints inferred
        # remove dlt columns
        actual = remove_dlt_columns(actual)
    elif backend == "pandas":
        # no precision, no nullability, all hints inferred
        # pandas destroys decimals
        expected = convert_non_pandas_types(expected)
    elif backend == "connectorx":
        expected = deepcopy(
            NULL_PRECISION_COLUMNS if nullable else NOT_NULL_PRECISION_COLUMNS
        )
        expected = convert_connectorx_types(expected)

    assert actual == expected


def convert_non_pandas_types(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    for column in columns:
        if column["data_type"] == "timestamp":
            column["precision"] = 6
    return columns


def remove_dlt_columns(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    return [col for col in columns if not col["name"].startswith("_dlt")]


def remove_default_precision(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    for column in columns:
        if column["data_type"] == "bigint" and column.get("precision") == 32:
            del column["precision"]
        if column["data_type"] == "text" and column.get("precision"):
            del column["precision"]
    return columns


def remove_timestamp_precision(
    columns: List[TColumnSchema], with_timestamps: bool = True
) -> List[TColumnSchema]:
    for column in columns:
        if (
            column["data_type"] == "timestamp"
            and column["precision"] == 6
            and with_timestamps
        ):
            del column["precision"]
        if column["data_type"] == "time" and column["precision"] == 6:
            del column["precision"]
    return columns


def convert_connectorx_types(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    """connector x converts decimals to double, otherwise tries to keep data types and precision
    nullability is not kept, string precision is not kept
    """
    for column in columns:
        if column["data_type"] == "bigint":
            if column["name"] == "int_col":
                column["precision"] = 32  # only int and bigint in connectorx
        if column["data_type"] == "text" and column.get("precision"):
            del column["precision"]
    return columns


def add_default_decimal_precision(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    for column in columns:
        if column["data_type"] == "decimal" and not column.get("precision"):
            column["precision"] = 38
            column["scale"] = 9
    return columns


PRECISION_COLUMNS = [
    {
        "data_type": "bigint",
        "name": "int_col",
    },
    {
        "data_type": "bigint",
        "name": "bigint_col",
    },
    {
        "data_type": "bigint",
        "precision": 32,
        "name": "smallint_col",
    },
    {
        "data_type": "decimal",
        "precision": 10,
        "scale": 2,
        "name": "numeric_col",
    },
    {
        "data_type": "decimal",
        "name": "numeric_default_col",
    },
    {
        "data_type": "text",
        "precision": 10,
        "name": "string_col",
    },
    {
        "data_type": "text",
        "name": "string_default_col",
    },
    {
        "data_type": "timestamp",
        "precision": 6,
        "name": "datetime_tz_col",
    },
    {
        "data_type": "timestamp",
        "precision": 6,
        "name": "datetime_ntz_col",
    },
    {
        "data_type": "date",
        "name": "date_col",
    },
    {
        "data_type": "time",
        "name": "time_col",
        "precision": 6,
    },
    {
        "data_type": "double",
        "name": "float_col",
    },
    {
        "data_type": "complex",
        "name": "json_col",
    },
    {
        "data_type": "bool",
        "name": "bool_col",
    },
]

NOT_NULL_PRECISION_COLUMNS = [
    {"nullable": False, **column} for column in PRECISION_COLUMNS
]
NULL_PRECISION_COLUMNS = [{"nullable": True, **column} for column in PRECISION_COLUMNS]

# but keep decimal precision
NO_PRECISION_COLUMNS = [
    {"name": column["name"], "data_type": column["data_type"]}
    if column["data_type"] != "decimal"
    else dict(column)
    for column in PRECISION_COLUMNS
]

NOT_NULL_NO_PRECISION_COLUMNS = [
    {"nullable": False, **column} for column in NO_PRECISION_COLUMNS
]
NULL_NO_PRECISION_COLUMNS = [
    {"nullable": True, **column} for column in NO_PRECISION_COLUMNS
]
