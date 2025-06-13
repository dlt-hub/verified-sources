import os
import pytest
from typing import List, Optional
import dlt
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny
from dlt.pipeline.pipeline import Pipeline
from sources.google_analytics import google_analytics, TIME_DIMENSIONS
from sources.google_analytics.settings import START_DATE
from sources.google_analytics import helpers
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    drop_active_pipeline_data,
)

QUERIES = [
    {
        "resource_name": "sample_analytics_data1",
        "dimensions": ["browser", "city"],
        "metrics": ["totalUsers", "transactions"],
    },
    {
        "resource_name": "sample_analytics_data2",
        "dimensions": ["browser", "city"],
        "metrics": ["totalUsers"],
    },
]

# dict containing the name of the tables expected in the db as keys and the number of rows expected as values
ALL_TABLES = {
    "dimensions": 388,
    "metrics": 101,
    "sample_analytics_data1": 12,
    "sample_analytics_data2": 12,
}
INCREMENTAL_SAVED_KEYS = [
    "last_load_sample_analytics_data1",
    "last_load_sample_analytics_data2",
]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    Checks to see that a full load yields expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly and check number of tables is correct
    pipeline = _create_pipeline(
        queries=QUERIES,
        destination_name=destination_name,
        dataset_name="analytics_dataset",
    )
    _check_pipeline_has_tables(pipeline=pipeline, tables=ALL_TABLES.keys())


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_metrics(destination_name: str) -> None:
    """
    Checks to see that metrics are saved correctly on the db.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly
    pipeline = _create_pipeline(
        queries=QUERIES,
        destination_name=destination_name,
        dataset_name="analytics_dataset",
    )
    with pipeline.sql_client() as client:
        sql_query = "SELECT api_name, category, description, ui_name FROM metrics;"
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == ALL_TABLES["metrics"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_dimension(destination_name: str) -> None:
    """
    Checks to see that dimensions are saved correctly on the db.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly
    pipeline = _create_pipeline(
        queries=QUERIES,
        destination_name=destination_name,
        dataset_name="analytics_dataset",
    )
    with pipeline.sql_client() as client:
        sql_query = "SELECT api_name, category, custom_definition, description, ui_name FROM dimensions;"
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == ALL_TABLES["dimensions"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_queries(destination_name: str) -> None:
    """
    Checks to see that report queries are saved correctly on the db.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly
    pipeline = _create_pipeline(
        queries=QUERIES,
        destination_name=destination_name,
        dataset_name="analytics_dataset",
    )
    with pipeline.sql_client() as client:
        # test 1st report
        sql_query = "SELECT browser, city, total_users_integer, transactions_integer FROM sample_analytics_data1;"
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == ALL_TABLES["sample_analytics_data1"]
        # test 2nd report
        sql_query = (
            "SELECT browser, city, total_users_integer FROM sample_analytics_data2;"
        )
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == ALL_TABLES["sample_analytics_data2"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incrementing(destination_name: str) -> None:
    """
    Checks that incremental loading works
    @:param: destination_name - redshift/bigquery/postgres
    """
    # Define all end_dates to be used for incremental loading, no new data expected in the last one
    first_end_date = pendulum.datetime(year=2021, month=12, day=1)
    second_end_date = pendulum.datetime(year=2021, month=12, day=31)
    third_end_date = pendulum.datetime(year=2023, month=1, day=1)
    fourth_end_date = pendulum.datetime(year=2023, month=2, day=1)
    incremental_end_dates = [
        first_end_date,
        second_end_date,
        third_end_date,
        fourth_end_date,
    ]

    incremental_load_counts = []

    data = google_analytics(queries=QUERIES)

    start_date = pendulum.parse(START_DATE)
    for incremental_end_date in incremental_end_dates:
        pipeline = dlt.pipeline(
            destination=destination_name,
            dev_mode=True,
            dataset_name="analytics_dataset",
        )
        for _, resource in data.resources.items():
            resource.apply_hints(
                incremental=dlt.sources.incremental(
                    "date",
                    primary_key=(),
                    initial_value=start_date,
                    end_value=incremental_end_date,
                    on_cursor_value_missing="include",
                )
            )

        info = pipeline.run(data)
        assert_load_info(info)
        incremental_load_counts.append(load_table_counts(pipeline, *ALL_TABLES.keys()))

    # Check new data is added after the 1st incremental load
    assert (
        incremental_load_counts[0] != ALL_TABLES
        and incremental_load_counts[1] != ALL_TABLES
    )
    _count_comparison(
        first_counts=incremental_load_counts[1],
        second_counts=incremental_load_counts[0],
        same_data_expected=False,
    )

    # Check new data is added after the 2nd incremental load
    assert (
        incremental_load_counts[1] != ALL_TABLES
        and incremental_load_counts[2] == ALL_TABLES
    )
    _count_comparison(
        first_counts=incremental_load_counts[2],
        second_counts=incremental_load_counts[1],
        same_data_expected=False,
    )

    # Check there is no new data after the 3rd incremental load
    assert incremental_load_counts[2] == incremental_load_counts[3]
    _count_comparison(
        first_counts=incremental_load_counts[3],
        second_counts=incremental_load_counts[2],
        same_data_expected=True,
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_pagination(destination_name: str) -> None:
    """
    Tests that pagination works as intended.
    :param destination_name:
    :return:
    """
    # Load the pipeline twice with different po
    os.environ["SOURCES__GOOGLE_ANALYTICS__GOOGLE_ANALYTICS__ROWS_PER_PAGE"] = "5"
    pipeline_pagination_5 = _create_pipeline(
        queries=QUERIES,
        destination_name=destination_name,
        dataset_name="analytics_dataset_pagination_5",
        dev_mode=True,
    )
    first_load_counts = load_table_counts(pipeline_pagination_5, *ALL_TABLES.keys())
    # drop the pipeline explicitly. fixture drops only one active pipeline
    # TODO: fix the drop_pipeline fixture to drop all pipelines created during test
    drop_active_pipeline_data()

    # do 2nd load of data and check that no new data is added, i.e. number of rows is the same
    os.environ["SOURCES__GOOGLE_ANALYTICS__GOOGLE_ANALYTICS__ROWS_PER_PAGE"] = "10"
    pipeline_pagination_10 = _create_pipeline(
        queries=QUERIES,
        destination_name=destination_name,
        dataset_name="analytics_dataset_pagination_10",
        dev_mode=True,
    )
    second_load_counts = load_table_counts(pipeline_pagination_10, *ALL_TABLES.keys())
    assert first_load_counts == second_load_counts
    assert first_load_counts == ALL_TABLES


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_starting_date(destination_name: str) -> None:
    """
    Tests that starting date works as intended.
    :param destination_name:
    :return:
    """
    # Load the pipeline twice with different starting dates, earlier starting date has more data with our current testing set: 18th April 2023.
    pipeline_start_date_1 = _create_pipeline(
        queries=QUERIES,
        destination_name=destination_name,
        dataset_name="analytics_dataset_start_date_1",
        dev_mode=True,
        start_date="2021-01-01",
    )
    first_load_counts = load_table_counts(pipeline_start_date_1, *ALL_TABLES.keys())
    drop_active_pipeline_data()

    pipeline_start_date_2 = _create_pipeline(
        queries=QUERIES,
        destination_name=destination_name,
        dataset_name="analytics_dataset_start_date_2",
        dev_mode=True,
        start_date="2022-01-01",
    )
    second_load_counts = load_table_counts(pipeline_start_date_2, *ALL_TABLES.keys())

    # first load_counts is expected to have more data, check for that
    assert second_load_counts != ALL_TABLES
    assert first_load_counts == ALL_TABLES

    _count_comparison(
        first_counts=first_load_counts,
        second_counts=second_load_counts,
        same_data_expected=False,
    )


@pytest.mark.parametrize("time_dimension", TIME_DIMENSIONS)
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_time_dimensions(destination_name: str, time_dimension: str, mocker) -> None:
    """
    Checks to see that time dimensions are correcly passed.
    :param destination_name: Name of the db the data is loaded to.
    :param time_dimension: Name of the time dimension.
    :param mocker: Mocker to spy the get_report function.
    :returns: None
    """
    if time_dimension == "dateHourMinute":
        pytest.skip("Property lacks data at dateHour granularity.")

    pipeline = dlt.pipeline(
        destination=destination_name,
        dev_mode=True,
        dataset_name="analytics_dataset",
    )

    QUERY = [
        {
            "resource_name": "sample_analytics_data1",
            "dimensions": ["browser", "city", time_dimension],
            "metrics": ["totalUsers", "transactions"],
        },
    ]

    EXPECTED_NUMBER_PER_TIME_DIMENSION = {
        "date": 12,
        "isoYearIsoWeek": 6,
        "year": 5,
        "yearMonth": 5,
        "dateHour": 88,
    }

    spy = mocker.spy(helpers, "get_report")

    data = google_analytics(queries=QUERY).with_resources("sample_analytics_data1")

    info = pipeline.run(data)
    assert_load_info(info)

    assert spy.call_count == 1
    assert len(spy.call_args_list[0].kwargs["dimension_list"]) == 3
    assert any(
        dimension.name == time_dimension
        for dimension in spy.call_args_list[0].kwargs["dimension_list"]
    )

    with pipeline.sql_client() as client:
        sql_query = f"""
            SELECT *
            FROM {pipeline.dataset_name}.sample_analytics_data1;
        """
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == EXPECTED_NUMBER_PER_TIME_DIMENSION[time_dimension]


def _create_pipeline(
    destination_name: str,
    dataset_name: str,
    queries: List[DictStrAny],
    start_date: Optional[str] = None,
    dev_mode: bool = True,
):
    """
    Helper, creates the pipelines and asserts the data is loaded correctly
    :param destination_name - redshift/bigquery/postgres
    :param start_date: Only used the first time for incremental loading to have a starting date for gathering data.
    :param dev_mode: pipeline parameter
    :param queries: Describes how many reports and what data should be retrieved for each report.
    """
    pipeline = dlt.pipeline(
        destination=destination_name,
        dev_mode=dev_mode,
        dataset_name=dataset_name,
    )
    # gather data with sources and see which data to run or not
    data = google_analytics(queries=queries, start_date=start_date)
    info = pipeline.run(data)
    assert_load_info(info)
    return pipeline


def _check_pipeline_has_tables(pipeline: Pipeline, tables: List[str]):
    """
    Helper that checks if a pipeline has all tables in the list and has the same number of proper tables as the list (child tables and dlt tables not included in this count)
    @:param pipeline: DLT pipeline
    @:param tables: list of expected table names
    """

    schema = pipeline.default_schema
    user_tables = schema.data_tables()
    assert set(tables).difference([t["name"] for t in user_tables]) == set()


def _count_comparison(
    first_counts: DictStrAny, second_counts: DictStrAny, same_data_expected: bool = True
):
    """
    Compares 2 dictionaries that have a row count for each loaded table. If the counts are supposed to have the same
    :param first_counts: Dict containing information about tables and respective rows per table
    :param second_counts: Dict containing information about tables and respective rows per table
    :param same_data_expected: If true, both tables are expected to have the same number of rows for non metadata tables, otherwise first_counts is expected to have more rows per table.
    :return:
    """
    assert len(first_counts) == len(second_counts)
    for table_name in first_counts:
        if table_name not in {"dimensions", "metrics"}:
            if same_data_expected:
                assert first_counts[table_name] == second_counts[table_name]
            else:
                assert first_counts[table_name] > second_counts[table_name]
