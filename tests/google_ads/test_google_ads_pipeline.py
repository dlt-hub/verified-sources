import os
import pytest
from typing import List, Optional
import dlt
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny
from dlt.common.utils import uniq_id
from dlt.pipeline.pipeline import Pipeline
from pipelines.google_ads import google_ads, google_ads_query
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
QUERIES = []
ALL_TABLES = []
QUERY_TABLES = []
DEFAULT_TABLES = []


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    Checks to see that a full load yields expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly and check number of tables is correct
    pipeline = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="test_google_ads_full_load", get_queries=True, get_default=True)
    _check_pipeline_has_tables(pipeline=pipeline, tables=ALL_TABLES)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_queries(destination_name: str) -> None:
    """
    Checks to see that metrics are saved correctly on the db.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly
    pipeline_queries = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="test_google_ads_queries", get_queries=True)
    _check_pipeline_has_tables(pipeline=pipeline_queries, tables=QUERY_TABLES)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_default(destination_name: str) -> None:
    """
    Checks to see that dimensions are saved correctly on the db.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly
    pipeline_default = _create_pipeline(destination_name=destination_name, dataset_name="test_google_ads_default_tables", get_default=True)
    _check_pipeline_has_tables(pipeline=pipeline_default, tables=DEFAULT_TABLES)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_destination_table(destination_name: str) -> None:
    """
    Checks to see that report queries are saved correctly on the db.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    pipeline_default = _create_pipeline(destination_name=destination_name, dataset_name="test_google_ads_dimensions_test", get_default=True)
    with pipeline_default.sql_client() as client:
        sql_query = "SELECT * FROM dimension_tables;"
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            pass


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_reporting_table(destination_name: str) -> None:
    """
    Checks that reporting table is loaded correctly.
    @:param: destination_name - redshift/bigquery/postgres
    """
    pipeline_default = _create_pipeline(destination_name=destination_name, dataset_name="test_google_ads_reports_test", get_default=True)
    with pipeline_default.sql_client() as client:
        sql_query = "SELECT * FROM reports_table;"
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            pass


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental(destination_name: str) -> None:
    """
    Checks that incremental loading works
    @:param: destination_name - redshift/bigquery/postgres
    """
    pass


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_pagination(destination_name: str) -> None:
    """
    Tests that pagination works as intended.
    :param destination_name:
    :return:
    """
    # Load the pipeline twice with different po
    os.environ["SOURCES__GOOGLE_ADS__ROWS_PER_PAGE"] = "5"
    pass


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_starting_date(destination_name: str) -> None:
    """
    Tests that starting date works as intended.
    :param destination_name:
    :return:
    """
    pass


def _create_pipeline(destination_name: str,
                     dataset_name: str, queries: List[DictStrAny],
                     start_date: Optional[str] = None,
                     full_refresh: bool = True,
                     get_default: bool = False,
                     get_queries: bool = False):
    """
    Helper, creates the pipelines and asserts the data is loaded correctly
    :param destination_name - redshift/bigquery/postgres
    :param start_date: Only used the first time for incremental loading to have a starting date for gathering data.
    :param full_refresh: pipeline parameter
    :param queries: Describes how many reports and what data should be retrieved for each report.
    :param get_default: Chooses if the default tables are to be loaded.
    :param get_queries: Chooses if the queries source should be loaded.
    """
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=full_refresh, dataset_name=dataset_name)
    # gather data with sources and see which data to run or not
    data = []
    if get_default:
        data.append(google_ads())
    if get_queries:
        data.append(google_ads_query())
    info = pipeline.run(data)
    assert_load_info(info, expected_load_packages=len(data))
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
    assert len(first_counts) == len (second_counts)
    for table_name in first_counts:
        if table_name not in {"dimensions", "metrics"}:
            if same_data_expected:
                assert first_counts[table_name] == second_counts[table_name]
            else:
                assert first_counts[table_name] > second_counts[table_name]
