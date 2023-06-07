import logging

import pytest
import dlt
from dlt.common.pipeline import LoadInfo
from sources.google_sheets import google_spreadsheet
from tests.utils import ALL_DESTINATIONS, assert_load_info


# list expected tables and the number of columns they are supposed to have
ALL_TABLES = [
    "all_types",
    "empty_row",
    "empty_rows",
    "has_empty",
    "hole_middle",
    "inconsistent_types",
    "more_data",
    "more_headers_than_data",
    "NamedRange1",
    "only_data",
    "only_headers",
    "Sheet 1",
    "sheet2",
    "sheet3",
    "sheet4",
    "two_tables",
]
COL_NUMS = [6, 5, 5, 5, 7, 5, 4, 8, 4, 2, 2, 4, 5, 9, 1, 9]
ALL_TABLES_LOADED = [
    "all_types",
    "empty_row",
    "empty_rows",
    "has_empty",
    "hole_middle",
    "inconsistent_types",
    "more_data",
    "more_headers_than_data",
    "named_range1",
    "only_data",
    "only_headers",
    "sheet_1",
    "sheet2",
    "sheet3",
    "sheet4",
    "spreadsheet_info",
    "two_tables",
]


def create_pipeline(
    destination_name,
    dataset_name,
    full_refresh=True,
    range_names=None,
    get_sheets=True,
    get_named_ranges=True,
) -> (LoadInfo, dlt.Pipeline):
    """
    Helper, creates a simple pipeline and returns it along with the load info.
    """
    pipeline = dlt.pipeline(
        destination=destination_name,
        full_refresh=full_refresh,
        dataset_name=dataset_name,
    )
    data = google_spreadsheet(
        range_names=range_names,
        get_sheets=get_sheets,
        get_named_ranges=get_named_ranges,
    )
    info = pipeline.run(data)
    return info, pipeline


def test_sample_load() -> None:
    """
    Tests access for a spreadsheet in config.toml and check that the pipeline was loaded correctly.
    """

    info = create_pipeline(
        destination_name="postgres",
        dataset_name="test_google_sheet_data",
        range_names=["Sheet 1"],
        get_sheets=False,
        get_named_ranges=False,
    )[0]
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    Sample pipeline run for all ranges in test1 spreadsheet. Checks the correct amount of tables is created in the database.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # FULL PIPELINE RUN
    info, pipeline = create_pipeline(
        destination_name=destination_name, dataset_name="test_full_load"
    )
    assert_load_info(info)

    # The schema should contain all listed tables
    # ALL_TABLES is missing spreadsheet info table - table being tested here
    schema = pipeline.default_schema
    user_tables = schema.data_tables()
    assert set([t["name"] for t in user_tables]) == set(ALL_TABLES_LOADED)

    # check load metadata
    with pipeline.sql_client() as c:
        # check every table has the correct name in the metadata table and the correct number of columns
        sql_query = "SELECT loaded_range, num_cols FROM spreadsheet_info ORDER BY LOWER(loaded_range);"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == len(ALL_TABLES)
            for i in range(len(rows)):
                assert rows[i][0] == ALL_TABLES[i]
                assert rows[i][1] == COL_NUMS[i]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_appending(destination_name) -> None:
    """
    Test that adding new data in the sheets will add new data to the destinations
    @:param: destination_name - redshift/bigquery/postgres
    """

    # Fetch ranges from pipeline and check
    test_ranges = ["Sheet 1!A1:D2", "Sheet 1!A1:D4"]
    test_ranges_table = ["sheet_1_a1_d2", "sheet_1_a1_d4"]

    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_appending",
        range_names=test_ranges,
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # TODO: decide what needs to be done when range is slightly increased
    # check table rows are appended
    with pipeline.sql_client() as c:
        sql_query1 = f"SELECT * FROM {test_ranges_table[0]};"
        sql_query2 = f"SELECT * FROM {test_ranges_table[1]};"
        with c.execute_query(sql_query1) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 1
        with c.execute_query(sql_query2) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_data_types(destination_name) -> None:
    """
    Test all data types are recognized correctly. Just checks all columns are formatted as the same data type.
    @:param: destination_name - redshift/bigquery/postgres
    """

    table_name_db = "all_types"
    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline_types = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_all_data_types",
        range_names=["all_types"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    schema = pipeline_types.default_schema
    assert table_name_db in schema.tables

    # pipeline doesn't reset schema.data_tables when run with other tests, so we have to check all the tables in the schema and check that the name matches
    test_table = schema.get_table(table_name_db)
    # check all columns
    assert test_table["columns"]["text_types"]["data_type"] == "text"
    assert test_table["columns"]["number_types"]["data_type"] == "bigint"
    assert test_table["columns"]["float_types"]["data_type"] == "double"
    assert test_table["columns"]["bool_types"]["data_type"] == "bool"
    assert test_table["columns"]["formula_types"]["data_type"] == "double"
    assert test_table["columns"]["date_types"]["data_type"] == "timestamp"


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_empty_row(destination_name) -> None:
    """
    Test ranges with a single empty row are processed correctly
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_empty_row",
        range_names=["empty_row"],
        get_named_ranges=False,
        get_sheets=False,
    )
    assert_load_info(info)

    # check table rows are appended
    with pipeline.sql_client() as c:
        sql_query = "SELECT * FROM empty_row;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 10


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_empty_rows(destination_name) -> None:
    """
    Test ranges with multiple empty rows are processed correctly
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_empty_rows",
        range_names=["empty_rows"],
        get_named_ranges=False,
        get_sheets=False,
    )
    assert_load_info(info)

    # check table rows are appended
    with pipeline.sql_client() as c:
        sql_query = "SELECT * FROM empty_rows;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 9


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_has_empty(destination_name) -> None:
    """
    Test ranges with random null values are processed correctly
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_has_empty",
        range_names=["has_empty"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # check table rows are appended
    with pipeline.sql_client() as c:
        sql_query1 = "SELECT * FROM has_empty;"
        check_null_query = "SELECT * FROM has_empty WHERE redi2 is Null OR test2 is Null or date_test is Null;"
        # check num rows
        with c.execute_query(sql_query1) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 9
        # check specific values are null
        with c.execute_query(check_null_query) as cur:
            rows = list(cur.fetchall())
            # only 3 rows with null values
            assert len(rows) == 3


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_inconsistent_types(destination_name) -> None:
    """
    Test ranges that have different data types in a single column
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_inconsistent_types",
        range_names=["inconsistent_types"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    with pipeline.sql_client() as c:
        sql_query = (
            "SELECT * FROM inconsistent_types WHERE "
            "test2__v_text is not Null "
            "OR redi2__v_double is not Null "
            "OR date_test__v_text is not Null "
            "OR bool_test__v_text is not Null;"
        )
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            # 4 rows with inconsistent types expected
            assert len(rows) == 4


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_more_headers(destination_name) -> None:
    """
    Test ranges that have more headers than data
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_more_headers",
        range_names=["more_headers_than_data"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    # run query to check number of columns
    with pipeline.sql_client() as c:
        sql_query = "SELECT * FROM more_headers_than_data;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            for row in rows:
                # each row must have 8 columns (including the 2 dlt ones)
                assert len(row) == 8


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_more_data(destination_name) -> None:
    """
    Test ranges that have more data than headers
    @:param: destination_name - redshift/bigquery/postgres
    """
    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_more_headers",
        range_names=["more_data"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)
    pipeline_schema = pipeline.default_schema
    logging.warning(pipeline_schema)
    with pipeline.sql_client() as c:
        sql_query = "SELECT * FROM more_data;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            for row in rows:
                # each row must have 6 columns(including the 2 dlt ones)
                assert len(row) == 6


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_two_tables(destination_name) -> None:
    """
    Test ranges that have 2 tables inside the range
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_two_tables",
        range_names=["two_tables"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    with pipeline.sql_client() as c:
        # this query will return all rows from 2nd table appended to the 1st table
        sql_query = "SELECT * FROM two_tables WHERE _10 is NULL;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            # 11 rows with inconsistent types expected
            assert len(rows) == 11


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_hole_middle(destination_name) -> None:
    """
    Test ranges that have 2 tables inside the range
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_hole_middle",
        range_names=["hole_middle"],
        get_sheets=False,
        get_named_ranges=False,
    )
    assert_load_info(info)

    with pipeline.sql_client() as c:
        # this query will return all rows from 2nd table appended to the 1st table
        sql_query = "SELECT * FROM hole_middle;"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            # 10 rows and 7 columns (including dlt ones) expected
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 7


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_named_range(destination_name) -> None:
    """
    Test that everything inside a named range is loaded correctly
    @:param: destination_name - redshift/bigquery/postgres
    """

    table_name_db = "named_range1"

    # run pipeline only for the specific table with all data types and grab that table
    # with these settings, the pipeline should only have the named_range1 table inside.
    info, pipeline = create_pipeline(
        destination_name=destination_name,
        dataset_name="test_named_range",
        get_sheets=False,
        get_named_ranges=True,
    )
    assert_load_info(info)

    # check columns have the correct data types in the schema
    # pipeline doesn't reset schema.data_tables when run with other tests, so we have to check all the tables in the schema and check that the name matches
    schema = pipeline.default_schema
    assert table_name_db in schema.tables
    test_table = schema.get_table(table_name_db)

    # check all column data types are correct
    assert test_table["columns"]["test3"]["data_type"] == "text"
    assert test_table["columns"]["_3"]["data_type"] == "bigint"
    assert test_table["columns"]["_1_03"]["data_type"] == "double"
    assert test_table["columns"]["true"]["data_type"] == "bool"

    # check all values are saved correctly
    expected_rows = [
        ("test4", 4, 1.04, True),
        ("test5", 5, 1.05, True),
        ("test6", 6, 1.06, True),
    ]
    # perform queries to check data inside
    with pipeline.sql_client() as c:
        sql_query = f"SELECT test3, _3, _1_03, true FROM {table_name_db};"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            # 3 rows and 4 columns expected
            assert len(rows) == 3
            for i in range(len(rows)):
                processed_row = _row_helper(rows[i], destination_name)
                assert processed_row == expected_rows[i]


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
