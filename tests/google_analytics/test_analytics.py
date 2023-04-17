import pytest
from typing import List
import dlt
from dlt.common.typing import DictStrAny
from dlt.pipeline.pipeline import Pipeline
from pipelines.google_analytics import google_analytics
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
QUERIES = [
    {"resource_name": "sample_analytics_data1", "dimensions": ["browser", "city"], "metrics": ["totalUsers", "transactions"]},
    {"resource_name": "sample_analytics_data2", "dimensions": ["browser", "city"], "metrics": ["totalUsers"]}
]
# dict containing the name of the tables expected in the db as keys and the number of rows expected as values
ALL_TABLES = {"dimensions": 206, "metrics": 96, "sample_analytics_data1": 12, "sample_analytics_data2": 12}
INCREMENTAL_SAVED_KEYS = ["last_load_sample_analytics_data1", "last_load_sample_analytics_data2"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    Checks to see that a full load yields expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly and check number of tables is correct
    pipeline = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="analytics_dataset")
    _check_pipeline_has_tables(pipeline=pipeline, tables=ALL_TABLES.keys())


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_metrics(destination_name: str) -> None:
    """
    Checks to see that metrics are saved correctly on the db.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    # load pipeline and check info is loaded correctly
    pipeline = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="analytics_dataset")
    with pipeline.sql_client() as client:
        sql_query = "SELECT api_name, category, custom_definition, description, expression, type, ui_name FROM metrics;"
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
    pipeline = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="analytics_dataset")
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
    pipeline = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="analytics_dataset")
    with pipeline.sql_client() as client:
        # test 1st report
        sql_query = "SELECT browser, city, total_users_integer, transactions_integer FROM sample_analytics_data1;"
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == ALL_TABLES["sample_analytics_data1"]
        # test 2nd report
        sql_query = "SELECT browser, city, total_users_integer FROM sample_analytics_data2;"
        with client.execute_query(sql_query) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == ALL_TABLES["sample_analytics_data2"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incrementing(destination_name: str) -> None:
    """
    Checks that incremental loading works
    @:param: destination_name - redshift/bigquery/postgres
    """
    # do 1st load of data, query the db and save the number of rows per resource
    pipeline = _create_pipeline(queries=QUERIES, destination_name=destination_name, dataset_name="analytics_dataset", full_refresh=False)
    first_load_counts = load_table_counts(pipeline, *ALL_TABLES.keys())

    # do 2nd load of data and check that no new data is added, i.e. number of rows is the same
    data = google_analytics(queries=QUERIES)
    info = pipeline.run(data)
    assert_load_info(info)
    incremental_load_counts = load_table_counts(pipeline, *ALL_TABLES.keys())

    assert first_load_counts == incremental_load_counts


def _create_pipeline(destination_name: str, dataset_name: str, queries: List[DictStrAny], full_refresh: bool = True):
    """
    Helper, creates the pipelines and asserts the data is loaded correctly
    @:param: destination_name - redshift/bigquery/postgres
    @:param: full_refresh: pipeline parameter
    @:param queries: Describes how many reports and what data should be retrieved for each report.
    """
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=full_refresh, dataset_name=dataset_name)
    # gather data with sources and see which data to run or not
    data = google_analytics(queries=queries)
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
