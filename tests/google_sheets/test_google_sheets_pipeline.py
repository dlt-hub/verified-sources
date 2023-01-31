import pytest
import dlt
from pipelines.google_sheets.google_sheets import google_spreadsheet
from tests.utils import ALL_DESTINATIONS, assert_load_info, drop_pipeline


TEST_SPREADSHEETS = ["1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU"]
ALL_TABLES = ["all_types", "empty_row", "empty_rows", "has_empty", "inconsistent_types", "more_headers_than_data", "more_data", "sheet1", "sheet2", "sheet3", "sheet4", "two_tables",
              "spreadsheet_info", "only_headers", "only_data", "hole_middle", "named_range1"]
ALL_DESTINATIONS = ["postgres"]


@pytest.mark.parametrize("spreadsheet_identifier", TEST_SPREADSHEETS)
def test_sample_load(spreadsheet_identifier) -> None:
    """
    Tests access for all spreadsheets in the spreadsheet list
    Also checks execution time for a pipeline run is less than 10 seconds
    @:param: spreadsheet_identifier -  id or url for a spreadsheet
    """

    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(destination="postgres", full_refresh=True, dataset_name="test_google_sheet_data")
    data = google_spreadsheet(spreadsheet_identifier=spreadsheet_identifier, range_names=["sheet1"], get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    Sample pipeline run for all ranges in test1 spreadsheet. Checks the correct amount of tables is created in the database.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_full_load")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0])
    info = pipeline.run(data)

    # The schema should contain all listed tables
    schema = pipeline.default_schema
    user_tables = schema.all_tables()
    assert len(user_tables) == len(ALL_TABLES)

    # check load metadata
    with pipeline.sql_client() as c:
        # you can use unqualified table names
        with c.execute_query("SELECT loaded_range, num_cols FROM spreadsheet_info ORDER BY loaded_range") as cur:
            rows = list(cur.fetchall())
            # this has all the tables created minus itself
            assert len(rows) == len(ALL_TABLES) - 1
            # check all tables loaded and check that they have the correct number of columns
            # spreadsheet_info doesn't count dlt tables, it just saves how many headers it spots from metadata, this number of cols doesn't necessarily match the ones in the db
            assert rows[0][1] == 6  # all_types table has 6 columns
            assert rows[1][1] == 5  # empty_row table has 5 columns
            assert rows[2][1] == 5  # empty_rows table has 5 columns
            assert rows[3][1] == 5  # has_empty table has 5 columns
            assert rows[4][1] == 7  # hole_middle table has 7 columns - this also includes the empty columns which are dropped from database
            assert rows[5][1] == 5  # inconsistent_types table has 5 columns
            assert rows[6][1] == 4  # more_data table has 4 columns
            assert rows[7][1] == 8  # more_headers_than_data table has 8 columns
            assert rows[8][1] == 4  # NamedRange1 has 4 columns
            assert rows[9][1] == 2  # only_data table has 2 columns
            assert rows[10][1] == 2  # only_headers table has 2 columns
            assert rows[11][1] == 4  # sheet1 table has 4 columns
            assert rows[12][1] == 5  # sheet2 table has 5 columns
            assert rows[13][1] == 9  # sheet3 table has 9 columns
            assert rows[14][1] == 1  # sheet4 table has 1 column
            assert rows[15][1] == 9  # two_tables table has 9 columns
    # make sure all jobs were loaded
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_appending(destination_name) -> None:
    """
    Test that adding new data in the sheets will add new data to the destinations
    @:param: destination_name - redshift/bigquery/postgres
    """

    # Fetch ranges from pipeline and check
    test_ranges = ["sheet1!A1:D2", "sheet1!A1:D4"]
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_appending")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=test_ranges)
    info = pipeline.run(data)
    # TODO: decide what needs to be done when range is slightly increased
    # check table rows are appended
    with pipeline.sql_client() as c:
        with c.execute_query("SELECT * FROM sheet1_A1_D2") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 1
        with c.execute_query("SELECT * FROM sheet1_A1_D4") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3
    # check loading is done correctly
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_data_types(destination_name) -> None:
    """
    Test all data types are recognized correctly. Just checks all columns are formatted as the same data type.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    pipeline_types = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_all_data_types")
    data_types = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["all_types"], get_named_ranges=False, get_sheets=False)
    info_types = pipeline_types.run(data_types)
    assert_load_info(info_types)

    schema = pipeline_types.default_schema
    # pipeline doesn't reset schema.all_tables when run with other tests, so we have to check all the tables in the schema and check that the name matches
    found_table = False
    for test_table in schema.all_tables():
        if test_table["name"] == "all_types":
            # check all columns
            assert test_table["columns"]["text_types"]["data_type"] == "text"
            assert test_table["columns"]["number_types"]["data_type"] == "bigint"
            assert test_table["columns"]["float_types"]["data_type"] == "double"
            assert test_table["columns"]["bool_types"]["data_type"] == "bool"
            assert test_table["columns"]["formula_types"]["data_type"] == "double"
            assert test_table["columns"]["date_types"]["data_type"] == "timestamp"
            found_table = True
    assert found_table


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_empty_row(destination_name) -> None:
    """
    Test ranges with a single empty row are processed correctly
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_empty_row")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["empty_row"], get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
    # check table rows are appended
    with pipeline.sql_client() as c:
        with c.execute_query("SELECT * FROM empty_row") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 10
    # check loading is done correctly
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_empty_rows(destination_name) -> None:
    """
    Test ranges with multiple empty rows are processed correctly
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_empty_rows")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["empty_rows"], get_named_ranges=False, get_sheets=False)
    info = pipeline.run(data)
    # check table rows are appended
    with pipeline.sql_client() as c:
        with c.execute_query("SELECT * FROM empty_rows") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 9
    # check loading is done correctly
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_has_empty(destination_name) -> None:
    """
    Test ranges with random null values are processed correctly
    @:param: destination_name - redshift/bigquery/postgres
    """

    # run pipeline only for the specific table with all data types and grab that table
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_has_empty")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["has_empty"], get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
    assert_load_info(info)

    # check table rows are appended
    with pipeline.sql_client() as c:
        # check num rows
        with c.execute_query("SELECT * FROM has_empty") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 9
        # check specific values are null
        check_null_query = "SELECT * FROM has_empty WHERE redi2 is Null OR test2 is Null or date_test is Null"
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
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_inconsistent_types")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["inconsistent_types"], get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
    assert_load_info(info)

    with pipeline.sql_client() as c:
        sql_query = "SELECT * FROM inconsistent_types WHERE " \
                    "test2__v_text is not Null " \
                    "OR redi2__v_double is not Null " \
                    "OR date_test__v_text is not Null " \
                    "OR bool_test__v_text is not Null;"
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
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_more_headers")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["more_headers_than_data"], get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
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
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_more_headers")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["more_headers_than_data"], get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
    assert_load_info(info)

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
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_two_tables")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["two_tables"], get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
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
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_hole_middle")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], range_names=["hole_middle"], get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
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
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_named_range")
    data = google_spreadsheet(spreadsheet_identifier=TEST_SPREADSHEETS[0], get_sheets=False, get_named_ranges=True)
    info = pipeline.run(data)
    assert_load_info(info)

    # check columns have the correct data types in the schema
    # pipeline doesn't reset schema.all_tables when run with other tests, so we have to check all the tables in the schema and check that the name matches
    schema = pipeline.default_schema
    found_table = False
    for test_table in schema.all_tables():
        if test_table["name"] == table_name_db:
            # check all column data types are correct
            assert test_table["columns"]["test3"]["data_type"] == "text"
            assert test_table["columns"]["_3"]["data_type"] == "bigint"
            assert test_table["columns"]["_1_03"]["data_type"] == "double"
            assert test_table["columns"]["true"]["data_type"] == "bool"
            found_table = True
    assert found_table

    expected_rows = [
        ("test4", 4, 1.04, True),
        ("test5", 5, 1.05, True),
        ("test6", 6, 1.06, True)
    ]

    # perform queries to check data inside
    with pipeline.sql_client() as c:
        sql_query = f"SELECT test3, _3, _1_03, true FROM {table_name_db};"
        with c.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            # 3 rows and 4 columns expected
            assert len(rows) == 3
            for i in range(len(rows)):
                assert rows[i] == expected_rows[i]
