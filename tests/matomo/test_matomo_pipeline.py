import os
import pytest
from typing import List, Optional
import dlt
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny
from dlt.pipeline.pipeline import Pipeline
from pipelines.matomo import matomo
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
QUERIES = []
# dict containing the name of the tables expected in the db as keys and the number of rows expected as values
ALL_TABLES = {}
INCREMENTAL_SAVED_KEYS = ["last_load_sample_analytics_data1", "last_load_sample_analytics_data2"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    Checks to see that a full load yields expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    pass


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_queries(destination_name: str) -> None:
    """
    Checks to see that report queries are saved correctly on the db.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly
    pipeline = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="matomo_dataset")
    with pipeline.sql_client() as client:
        # test 1st report
        sql_query = "SELECT * FROM sample_analytics_data1;"
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == ALL_TABLES["sample_analytics_data1"]
        # test 2nd report
        sql_query = "SELECT * FROM sample_analytics_data2;"
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
    incremental_end_dates = [first_end_date, second_end_date, third_end_date, fourth_end_date]

    incremental_load_counts = []
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="analytics_dataset")

    # load the rest of the data
    for incremental_end_date in incremental_end_dates:
        with pendulum.test(incremental_end_date):
            data = matomo(queries=QUERIES)
            info = pipeline.run(data)
            assert_load_info(info)
            incremental_load_counts.append(load_table_counts(pipeline, *ALL_TABLES.keys()))

    # Check new data is added after the 1st incremental load
    assert incremental_load_counts[0] != ALL_TABLES and incremental_load_counts[1] != ALL_TABLES
    _count_comparison(first_counts=incremental_load_counts[1], second_counts=incremental_load_counts[0], same_data_expected=False)

    # Check new data is added after the 2nd incremental load
    assert incremental_load_counts[1] != ALL_TABLES and incremental_load_counts[2] == ALL_TABLES
    _count_comparison(first_counts=incremental_load_counts[2], second_counts=incremental_load_counts[1], same_data_expected=False)

    # Check there is no new data after the 3rd incremental load
    assert incremental_load_counts[2] == incremental_load_counts[3]
    _count_comparison(first_counts=incremental_load_counts[3], second_counts=incremental_load_counts[2], same_data_expected=True)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_starting_date(destination_name: str) -> None:
    """
    Tests that starting date works as intended.
    :param destination_name:
    :return:
    """
    # Load the pipeline twice with different starting dates, earlier starting date has more data with our current testing set: 18th April 2023.
    pipeline_start_date_1 = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="matomo_dataset_start_date_1", full_refresh=True, start_date="2021-01-01")
    first_load_counts = load_table_counts(pipeline_start_date_1, *ALL_TABLES.keys())

    pipeline_start_date_2 = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="matomo_dataset_start_date_2", full_refresh=True, start_date="2022-01-01")
    second_load_counts = load_table_counts(pipeline_start_date_2, *ALL_TABLES.keys())

    # first load_counts is expected to have more data, check for that
    assert second_load_counts != ALL_TABLES and first_load_counts == ALL_TABLES
    _count_comparison(first_counts=first_load_counts, second_counts=second_load_counts, same_data_expected=False)


def _create_pipeline(destination_name: str, dataset_name: str, queries: List[DictStrAny], start_date: Optional[str] = None, full_refresh: bool = True):
    """
    Helper, creates the pipelines and asserts the data is loaded correctly
    :param destination_name - redshift/bigquery/postgres
    :param start_date: Only used the first time for incremental loading to have a starting date for gathering data.
    :param full_refresh: pipeline parameter
    :param queries: Describes how many reports and what data should be retrieved for each report.
    """
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=full_refresh, dataset_name=dataset_name)
    # gather data with sources and see which data to run or not
    data = matomo(queries=queries, start_date=start_date)
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


def _count_comparison(first_counts: DictStrAny, second_counts: DictStrAny, same_data_expected: bool = True):
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
