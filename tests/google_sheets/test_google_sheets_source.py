from typing import Tuple

import pytest
import dlt
from datetime import date # noqa: I251
from dlt.common.pipeline import LoadInfo
from sources.google_sheets import google_spreadsheet
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    assert_query_data,
    load_table_counts,
)


# list expected tables and the number of columns they are supposed to have
ALL_RANGES = {
    "empty",
    "all_types",
    "empty_row",
    "empty_rows",
    "has_empty",
    "hole_middle",
    "table_in_middle",
    "inconsistent_types",
    "more_data",
    "more_headers_than_data",
    "only_data",
    "only_headers",
    "Sheet 1",
    "sheet2",
    "sheet3",
    "sheet4",
    "two_tables",
    "hidden_columns_merged_cells",
    "Blank Columns",
    "trailing_empty_cols_1",
    "trailing_empty_cols_2",
    "trailing_empty_cols_3",
    "trailing_empty_col_date",
}

SKIPPED_RANGES = {
    "empty",
    "only_data",
    "only_headers",
}

NAMED_RANGES = {
    "NamedRange1",
    "NamedRange2",
}

SHEETS = ALL_RANGES - NAMED_RANGES

ALL_TABLES_LOADED = {
    "all_types",
    "empty_row",
    "empty_rows",
    "has_empty",
    "hole_middle",
    "table_in_middle",
    "inconsistent_types",
    "more_data",
    "more_headers_than_data",
    "sheet_1",
    "sheet2",
    "sheet3",
    "sheet4",
    "spreadsheet_info",
    "two_tables",
    "hidden_columns_merged_cells",
    "blank_columns",
    "trailing_empty_cols_1",
    "trailing_empty_cols_2",
    "trailing_empty_cols_3",
    "trailing_empty_col_date",
}


def test_single_explicit_range_load() -> None:
    """
    Tests access for a spreadsheet in config.toml and check that the pipeline was loaded correctly.
    """

    info, pipeline = _run_pipeline(
        destination_name="postgres",
        dataset_name="test_google_sheet_data",
        range_names=["Sheet 1"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)
    user_tables = pipeline.default_schema.data_tables()
    assert set([t["name"] for t in user_tables]) == {"sheet_1", "spreadsheet_info"}


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    Sample pipeline run for all ranges in test1 spreadsheet. Checks the correct amount of tables is created in the database.
    """

    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_full_load",
        get_sheets=True,
        get_named_ranges=False,
        range_names=[],
    )
    assert_load_info(info)

    # The schema should contain all listed tables
    # ALL_TABLES is missing spreadsheet info table - table being tested here
    schema = pipeline.default_schema
    user_tables = schema.data_tables()
    user_table_names = set([t["name"] for t in user_tables])
    assert user_table_names == ALL_TABLES_LOADED

    # check load metadata
    with pipeline.sql_client() as c:
        # check every table has the correct name in the metadata table
        sql_query = "SELECT range_name, skipped FROM spreadsheet_info"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            loaded_ranges = [r[0] for r in rows]
            assert set(loaded_ranges) == ALL_RANGES
            skipped_ranges = [r[0] for r in rows if r[1]]
            assert set(skipped_ranges) == SKIPPED_RANGES


def test_get_named_ranges() -> None:
    # take data from spreadsheet 1
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        get_named_ranges=True,
        get_sheets=False,
    )
    assert set(data.resources.keys()) == {"NamedRange1", "spreadsheet_info"}


def test_get_sheets() -> None:
    # take data from spreadsheet 1
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        get_named_ranges=False,
        get_sheets=True,
    )
    assert set(data.resources.keys()) - {"spreadsheet_info"} == SHEETS - SKIPPED_RANGES


def test_get_fancy_sheets() -> None:
    # take data from spreadsheet 1
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        get_named_ranges=False,
        get_sheets=True,
    )
    assert set(data.resources.keys()) - {"spreadsheet_info"} == SHEETS - SKIPPED_RANGES


def test_blank_columns() -> None:
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        get_named_ranges=False,
        get_sheets=False,
        range_names=["Blank Columns"],
    )
    pipeline = dlt.pipeline(
        destination="duckdb",
        dev_mode=True,
        dataset_name="test_blank_columns_data",
    )
    pipeline.extract(data)
    pipeline.normalize()
    # there were two blank columns that got automatic col_n name but contained no data so dlt eliminated them
    assert set(pipeline.default_schema.get_table_columns("blank_columns").keys()) == {
        "vergleich",
        "anbieter",
        "art",
        "grundpreis_mtl",
        "verbrauch",
        "jahreskosten",
        "netto_mtl_kosten",
        "brutto_mtl_kosten",
        "_dlt_id",
        "_dlt_load_id",
    }


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_hidden_columns_merged_cells(destination_name) -> None:
    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_hidden_columns_merged_cells",
        range_names=["hidden_columns_merged_cells"],
        get_named_ranges=False,
        get_sheets=False,
    )
    assert_load_info(info)

    # merged cells produce empty values but number of rows stays the same
    assert load_table_counts(pipeline, "hidden_columns_merged_cells") == {
        "hidden_columns_merged_cells": 7
    }

    # hidden columns are returned
    assert "art" in pipeline.default_schema.get_table_columns(
        "hidden_columns_merged_cells"
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_custom_ranges(destination_name) -> None:
    """
    Test that adding new data in the sheets will add new data to the destinations
    """

    # Fetch ranges from pipeline and check
    test_ranges = ["Sheet 1!A1:D2", "Sheet 1!A1:D4"]
    test_ranges_table = ["sheet_1_a1_d2", "sheet_1_a1_d4"]

    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_custom_ranges",
        range_names=test_ranges,
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # test2 column is missing because it contains no data
    assert set(pipeline.default_schema.get_table_columns("sheet_1_a1_d2").keys()) == {
        "test",
        "add1",
        "add2",
        "_dlt_id",
        "_dlt_load_id",
    }
    # in the second table a variant column is created
    assert set(pipeline.default_schema.get_table_columns("sheet_1_a1_d4").keys()) == {
        "test",
        "add1",
        "add2",
        "add2__v_text",
        "_dlt_id",
        "_dlt_load_id",
    }

    # check table rows are appended
    with pipeline.sql_client() as c:
        sql_query1 = f"SELECT * FROM {test_ranges_table[0]};"
        with c.execute_query(sql_query1) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 1
            assert rows[0][:-2] == (3, 3, 3.6)

        sql_query2 = f"SELECT * FROM {test_ranges_table[1]} ORDER BY test;"
        with c.execute_query(sql_query2) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3
            # check variant column value
            assert rows[1][-1] == '"test"'


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_data_types(destination_name) -> None:
    """
    Test all data types are recognized correctly. Just checks all columns are formatted as the same data type.
    """

    table_name_db = "all_types"
    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline_types = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_all_data_types",
        range_names=["all_types"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    schema = pipeline_types.default_schema
    assert table_name_db in schema.tables

    test_table = schema.get_table(table_name_db)
    # check all columns
    assert test_table["columns"]["text_types"]["data_type"] == "text"
    assert test_table["columns"]["number_types"]["data_type"] == "bigint"
    assert test_table["columns"]["float_types"]["data_type"] == "double"
    assert test_table["columns"]["bool_types"]["data_type"] == "bool"
    assert test_table["columns"]["formula_types"]["data_type"] == "double"
    assert test_table["columns"]["datetime_types"]["data_type"] == "timestamp"
    assert test_table["columns"]["date_types"]["data_type"] == "date"
    assert test_table["columns"]["time_types"]["data_type"] == "timestamp"


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_empty_rows(destination_name) -> None:
    """
    Test ranges with a single empty row are processed correctly
    """

    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_empty_row",
        range_names=["empty_row", "empty_rows"],
        get_named_ranges=False,
        get_sheets=False,
    )
    assert_load_info(info)

    # check table rows are appended
    assert load_table_counts(pipeline, "empty_row", "empty_rows") == {
        "empty_row": 10,
        "empty_rows": 9,
    }


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_has_nulls_for_empty_cells(destination_name) -> None:
    """
    Test ranges with random null values are processed correctly
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_has_empty",
        range_names=["has_empty"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # check table rows are appended
    with pipeline.sql_client() as c:
        check_null_query = "SELECT * FROM has_empty WHERE redi2 is Null OR test2 is Null or date_test is Null;"
        # check specific values are null
        with c.execute_query(check_null_query) as cur:
            rows = list(cur.fetchall())
            # only 3 rows with null values
            assert len(rows) == 3


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_inconsistent_types(destination_name) -> None:
    """
    Test ranges that have different data types in a single column
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_inconsistent_types",
        range_names=["inconsistent_types"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)
    # mind that "date_test" column has bool and text variants
    assert set(
        pipeline.default_schema.get_table_columns("inconsistent_types").keys()
    ) == {
        "float_test",
        "bool_test",
        "test2",
        "date_test__v_text",
        "redi2",
        "bool_test__v_text",
        "test2__v_text",
        "_dlt_load_id",
        "_dlt_id",
        # "date_test__v_bool", # pipeline doesn't have this type any more
        "redi2__v_double",
        "date_test",
    }

    with pipeline.sql_client() as c:
        sql_query = (
            "SELECT * FROM inconsistent_types WHERE "
            "test2__v_text is not Null "
            "OR redi2__v_double is not Null "
            "OR date_test__v_text is not Null "
            # "OR date_test__v_bool is not Null" pipeline doesn't have this type any more
            "OR bool_test__v_text is not Null;"
        )
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            # 4 rows with inconsistent types expected
            assert len(rows) == 4


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_more_headers_than_data(destination_name) -> None:
    """
    Test ranges that have more headers than data. Columns with headers and without data are dropped.
    """
    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_more_headers",
        range_names=["more_headers_than_data"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # no extra columns in schema
    assert {"extra_header", "extra_header2"}.intersection(
        set(pipeline.default_schema.get_table_columns("more_headers_than_data").keys())
    ) == set()

    # run query to check number of columns
    with pipeline.sql_client() as c:
        sql_query = "SELECT * FROM more_headers_than_data;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            for row in rows:
                # each row must have 8 columns (including the 2 dlt ones)
                assert len(row) == 8


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_more_data_than_headers(destination_name) -> None:
    """
    Test ranges that have more data than headers. Columns without headers will be dropped!
    """
    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_more_headers",
        range_names=["more_data"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)
    assert set(pipeline.default_schema.get_table_columns("more_data").keys()) == {
        "text_types",
        "number_types",
        "float_types",
        "bool_types",
        "_dlt_load_id",
        "_dlt_id",
    }


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_two_overlapping_tables(destination_name) -> None:
    """
    Test ranges that have 2 tables inside the range - standard rules apply so only parts of the second table overlapping with columns of the first table will appear
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_two_tables",
        range_names=["two_tables"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # all the headers are automatic
    headers = [
        h
        for h in pipeline.default_schema.get_table_columns("two_tables").keys()
        if h.startswith("col")
    ]
    assert len(headers) == 9

    # number of rows from two tables
    assert load_table_counts(pipeline, "two_tables") == {"two_tables": 22}

    # assert first column
    assert_query_data(
        pipeline,
        "SELECT col_1 FROM two_tables ORDER BY col_1 NULLS FIRST",
        [None] * 11 + list(range(10, 21)),
    )
    # assert first overlapped column
    assert_query_data(
        pipeline, "SELECT col_7 FROM two_tables ORDER BY col_7 ASC", list(range(1, 23))
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_hole_middle(destination_name) -> None:
    """
    Test ranges that have 2 tables inside the range
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_hole_middle",
        range_names=["hole_middle"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)
    assert load_table_counts(pipeline, "hole_middle") == {"hole_middle": 11}
    headers = [
        h
        for h in pipeline.default_schema.get_table_columns("hole_middle").keys()
        if h.startswith("col")
    ]
    assert len(headers) == 5


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_explicit_named_range(destination_name) -> None:
    """
    Test that everything inside a named range is loaded correctly
    @:param: destination_name - redshift/bigquery/postgres
    """

    table_name_db = "named_range1"

    # run pipeline only for the specific table with all data types and grab that table
    # with these settings, the pipeline should only have the named_range1 table inside.
    info, pipeline = _run_pipeline(
        destination_name=destination_name,
        dataset_name="test_named_range",
        range_names=["NamedRange1", "NamedRange2"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # check columns have the correct data types in the schema
    schema = pipeline.default_schema
    assert table_name_db in schema.tables
    # this one was skipped
    assert "named_range2" not in schema.tables
    test_table = schema.get_table(table_name_db)

    # check all column data types are correct - also order must match
    assert [
        c["data_type"]
        for c in test_table["columns"].values()
        if not c["name"].startswith("_dlt")
    ] == ["text", "bigint", "double", "bool"]

    # check all values are saved correctly
    expected_rows = [
        ("test3", 3, 1.03, True),
        ("test4", 4, 1.04, True),
        ("test5", 5, 1.05, True),
        ("test6", 6, 1.06, True),
    ]

    # perform queries to check data inside
    with pipeline.sql_client() as c:
        quoted_range = c.capabilities.escape_identifier("range")
        # columns are auto named - we hit a middle of a table with this range
        sql_query = f"SELECT col_1, col_2, col_3, col_4 FROM {table_name_db};"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 4
            for i in range(len(rows)):
                processed_row = _row_helper(rows[i], destination_name)
                assert processed_row == expected_rows[i]

    # check spreadsheet info
    assert_query_data(
        pipeline,
        f"SELECT {quoted_range} FROM spreadsheet_info ORDER BY {quoted_range} ASC",
        ["empty!ZY1:AAA4", "more_data!A4:D7"],
    )


def test_invalid_range():
    with pytest.raises(Exception) as py_ex:
        _run_pipeline(
            destination_name="duckdb",
            dataset_name="test_named_range",
            range_names=["NamedRangeNotExists"],
            get_sheets=False,
            get_named_ranges=False,
        )
    assert "Unable to parse range: NamedRangeNotExists" in str(py_ex.value)


def test_auto_header_names():
    pass


def test_table_rename() -> None:
    pipeline = dlt.pipeline(
        destination="duckdb",
        dev_mode=True,
        dataset_name="test_table_rename_data",
    )
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        range_names=["Sheet 1!A1:B10"],
        get_named_ranges=False,
    )
    # apply the table name to the existing resource: the resource name is the name of the range
    data.resources["Sheet 1!A1:B10"].apply_hints(table_name="my_a1_data")
    info = pipeline.run(data)
    assert_load_info(info)
    user_tables = pipeline.default_schema.data_tables()
    # check if table is there
    assert set([t["name"] for t in user_tables]) == {"my_a1_data", "spreadsheet_info"}


def test_table_rename_and_multiple_spreadsheets() -> None:
    pipeline = dlt.pipeline(
        destination="duckdb",
        dev_mode=True,
        dataset_name="test_table_rename_data",
    )
    # take data from spreadsheet 1
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        range_names=["Sheet 1!A1:B10"],
        get_named_ranges=False,
    )

    # take data from spreadsheet 2
    data_2 = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        range_names=["Sheet 1!B1:C10"],
        get_named_ranges=False,
    )
    # apply the table name to the existing resource: the resource name is the name of the range
    data.resources["Sheet 1!A1:B10"].apply_hints(table_name="my_a1_data")
    data_2.resources["Sheet 1!B1:C10"].apply_hints(table_name="second_sheet_data")

    # load two spreadsheets
    info = pipeline.run([data, data_2])
    assert_load_info(info)
    user_tables = pipeline.default_schema.data_tables()
    # check if table is there
    assert set([t["name"] for t in user_tables]) == {
        "my_a1_data",
        "second_sheet_data",
        "spreadsheet_info",
    }


def test_no_ranges():
    # no ranges to extract
    info, pipeline = _run_pipeline(
        destination_name="duckdb",
        dataset_name="test_table_in_middle",
        range_names=[],
        get_sheets=False,
        get_named_ranges=False,
    )
    # spreadsheet_info was empty so not created
    assert pipeline.default_schema.data_tables() == []
    assert_load_info(info)


def test_table_not_A1():
    # make sure all data is loaded when it does not start at A1
    info, pipeline = _run_pipeline(
        destination_name="duckdb",
        dataset_name="test_table_in_middle",
        range_names=["table_in_middle"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # 11 rows
    assert load_table_counts(pipeline, "table_in_middle") == {"table_in_middle": 11}
    # 9 auto generated headers
    headers = [
        h
        for h in pipeline.default_schema.get_table_columns("table_in_middle").keys()
        if h.startswith("col")
    ]
    assert len(headers) == 9
    # check range
    assert_query_data(
        pipeline,
        "SELECT range FROM spreadsheet_info ORDER BY range ASC",
        ["table_in_middle!AB9:AJ1000"],
    )
    # check first column
    assert_query_data(
        pipeline,
        "SELECT col_1 FROM table_in_middle ORDER BY col_1 ASC",
        list(map(str, range(11, 21))) + ["AB9_head"],
    )
    # check last column
    assert_query_data(
        pipeline, "SELECT col_9 FROM table_in_middle ORDER BY col_9 ASC", range(90, 101)
    )


def test_trailing_empty_cols() -> None:
    info, pipeline = _run_pipeline(
        destination_name="duckdb",
        dataset_name="test_trailing_empty_cols",
        range_names=[
            "trailing_empty_cols_1",
            "trailing_empty_cols_2",
            "trailing_empty_cols_3",
        ],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    assert "trailing_empty_cols_1" in pipeline.default_schema.tables
    assert "trailing_empty_cols_2" in pipeline.default_schema.tables
    assert "trailing_empty_cols_3" in pipeline.default_schema.tables

    assert set(
        pipeline.default_schema.get_table_columns("trailing_empty_cols_1").keys()
    ) == {"col0", "col1", "col2", "_dlt_id", "_dlt_load_id"}
    assert set(
        pipeline.default_schema.get_table_columns("trailing_empty_cols_2").keys()
    ) == {
        "col0",
        "col1",
        "col2",
        "col3",
        "col3__v_text",
        "col4",
        "_dlt_id",
        "_dlt_load_id",
    }
    assert set(
        pipeline.default_schema.get_table_columns("trailing_empty_cols_3").keys()
    ) == {
        "col0",
        "col1",
        "col2",
        "col3",
        "col3__v_text",
        "col4",
        "col5",
        "_dlt_id",
        "_dlt_load_id",
    }

    expected_rows = [
        (322, None, None, 2, None, None, 123456),
        (43, "dsa", "dd", None, "w", 2, None),
        (432, "scds", "ddd", None, "e", 3, None),
        (None, "dsfdf", "dddd", None, "r", 4, None),
    ]

    with pipeline.sql_client() as c:
        sql_query = "SELECT col0, col1, col2 FROM trailing_empty_cols_1;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 4
            assert rows == [row[:3] for row in expected_rows]

        sql_query = "SELECT col0, col1, col2, col3, col3__v_text, col4 FROM trailing_empty_cols_2;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 4
            assert rows == [row[:6] for row in expected_rows]

        sql_query = "SELECT col0, col1, col2, col3, col3__v_text, col4, col5 FROM trailing_empty_cols_3;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 4
            assert rows == expected_rows


@pytest.mark.parametrize("with_hints", [True, False])
def test_trailing_empty_col_date(with_hints: bool) -> None:
    pipeline = dlt.pipeline(
        destination="duckdb",
        dev_mode=True,
        dataset_name="test_trailing_empty_col_date",
    )
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        range_names=["trailing_empty_cols_1", "trailing_empty_col_date"],
        get_named_ranges=False,
    )
    if with_hints:
        data.trailing_empty_col_date.apply_hints(
            columns={"Start Date": {"data_type": "date"}}
        )
    info = pipeline.run(data)
    assert_load_info(info)

    assert "trailing_empty_col_date" in pipeline.default_schema.tables
    assert set(
        pipeline.default_schema.get_table_columns("trailing_empty_col_date").keys()
    ) == {"start_date", "end_date", "text", "_dlt_id", "_dlt_load_id"}

    expected = [
        (None, date(2027, 4, 12), "blablabla"),
        (
            date(2028, 4, 12) if with_hints else 46855,
            date(2027, 4, 12),
            "43432",
        ),
    ]
    with pipeline.sql_client() as c:
        sql_query = f"SELECT start_date, end_date, text FROM {pipeline.dataset_name}.trailing_empty_col_date;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 2
            assert rows == expected


def _row_helper(row, destination_name):
    """
    Helper, unpacks the rows from different databases (Bigquery, Postgres, Redshift) to a tuple
    @:param: row - the row returned from the sql query
    @:param: destination_name - the name of the database
    @:returns: all the values in the row as a tuple
    """

    if destination_name == "bigquery":
        return tuple([val for val in row.values()])
    else:
        # redshift & postgres
        return row


def _run_pipeline(
    destination_name,
    dataset_name,
    dev_mode=True,
    range_names=None,
    get_sheets=True,
    get_named_ranges=True,
) -> Tuple[LoadInfo, dlt.Pipeline]:
    """
    Helper, creates a simple pipeline and returns it along with the load info.
    """
    pipeline = dlt.pipeline(
        destination=destination_name,
        dev_mode=dev_mode,
        dataset_name=dataset_name,
    )
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        range_names=range_names,
        get_sheets=get_sheets,
        get_named_ranges=get_named_ranges,
    )
    info = pipeline.run(data)
    return info, pipeline
