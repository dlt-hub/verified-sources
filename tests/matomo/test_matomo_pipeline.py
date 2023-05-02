import pytest
from typing import List
import dlt
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny
from dlt.pipeline.pipeline import Pipeline
from pipelines.matomo import matomo_events, matomo_reports
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


QUERIES = [
    {
        "resource_name": "sample_analytics_data1",
        "methods": ["VisitsSummary.get", "API.getReportMetadata"],
        "date": "2020-01-01",
        "period": "day",
        "extra_params": {}
    },
    {
        "resource_name": "sample_analytics_data2",
        "methods": ["VisitsSummary.get", "API.getReportMetadata", "CustomReports.getCustomReport"],
        "date": "2022-01-01",
        "period": "day",
        "extra_params": {"idCustomReport": 1}
    }
]
INCREMENTAL_END_DATES = [
    pendulum.datetime(year=2020, month=1, day=1),
    pendulum.datetime(year=2020, month=2, day=1),
    pendulum.datetime(year=2021, month=1, day=1),
    pendulum.datetime(year=2021, month=2, day=1)
]
QUERIES_START_DATE1 = [
        {
            "resource_name": "sample_analytics_data1",
            "methods": ["VisitsSummary.get"],
            "date": "2020-01-01",
            "period": "day",
            "extra_params": {}
        }
]
QUERIES_START_DATE2 = [
        {
            "resource_name": "sample_analytics_data1",
            "methods": ["VisitsSummary.get"],
            "date": "2022-01-01",
            "period": "day",
            "extra_params": {}
        }
]
ALL_TABLES_START_DATE = ["sample_analytics_data1_visits_summary_get"]
REPORTS_SITE_ID = 3
LIVE_EVENTS_SITE_ID = 3
# dict containing the name of the tables expected in the db as keys and the number of rows expected as values
ALL_TABLES_REPORTS = ["sample_analytics_data1_visits_summary_get", "sample_analytics_data1_api_get_report_metadata", "sample_analytics_data2_visits_summary_get",
                      "sample_analytics_data2_api_get_report_metadata", "sample_analytics_data2_custom_reports_get_custom_report"]
ALL_TABLES_EVENTS = ["visitors", "visits"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_reports(destination_name: str) -> None:
    """
    Checks to see that loading regular reports yields expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """

    pipeline = _create_pipeline(destination_name=destination_name, dataset_name="matomo_dataset", include_reports=True, include_events=False, queries=QUERIES)
    _check_pipeline_has_tables(pipeline, ALL_TABLES_REPORTS)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_events(destination_name: str) -> None:
    """
    Checks to see that loading live events will yield expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """

    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="matomo_dataset")
    data_events = matomo_events(live_events_site_id=LIVE_EVENTS_SITE_ID, get_live_event_visitors=True)
    info = pipeline.run(data_events)
    assert_load_info(info)
    _check_pipeline_has_tables(pipeline, ALL_TABLES_EVENTS)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incrementing_reports(destination_name: str) -> None:
    """
    Checks that incremental loading works for reports
    @:param: destination_name - redshift/bigquery/postgres
    """
    incremental_load_counts = []
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="matomo")

    # load the rest of the data
    for incremental_end_date in INCREMENTAL_END_DATES:
        with pendulum.test(incremental_end_date):
            data = matomo_reports(queries=QUERIES, site_id=REPORTS_SITE_ID)
            info = pipeline.run(data)
            assert_load_info(info)
            incremental_load_counts.append(load_table_counts(pipeline, *ALL_TABLES_REPORTS))

    # Check new data is added after each incremental load
    _count_comparison(first_counts=incremental_load_counts[1], second_counts=incremental_load_counts[0], same_data_expected=False)
    _count_comparison(first_counts=incremental_load_counts[2], second_counts=incremental_load_counts[1], same_data_expected=False)
    _count_comparison(first_counts=incremental_load_counts[3], second_counts=incremental_load_counts[2], same_data_expected=False)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_start_date(destination_name: str) -> None:
    """
    Checks that given starting dates work in matomo reports
    @:param: destination_name - redshift/bigquery/postgres
    """

    pipeline1 = _create_pipeline(destination_name=destination_name, dataset_name="matomo_start_date1", full_refresh=True, include_reports=True, include_events=False, queries=QUERIES_START_DATE1)
    load_count1 = load_table_counts(pipeline1, *ALL_TABLES_START_DATE)
    pipeline2 = _create_pipeline(destination_name=destination_name, dataset_name="matomo_start_date2", full_refresh=True, include_reports=True, include_events=False, queries=QUERIES_START_DATE2)
    load_count2 = load_table_counts(pipeline2, *ALL_TABLES_START_DATE)

    # Check new data is added after each incremental load
    _count_comparison(first_counts=load_count1, second_counts=load_count2, same_data_expected=False)


def _create_pipeline(destination_name: str, dataset_name: str, queries: List[DictStrAny], full_refresh: bool = True, include_reports: bool = False, include_events: bool = False):
    """
    Helper, creates the pipelines and asserts the data is loaded correctly
    :param destination_name - redshift/bigquery/postgres
    :param full_refresh: pipeline parameter
    :param queries: Describes how many reports and what data should be retrieved for each report.
    :param include_reports: Bool to include the reports source
    :param include_events: Bool to include the live events source
    """
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=full_refresh, dataset_name=dataset_name)
    data_sources = []
    if include_reports:
        data_reports = matomo_reports(queries=queries, site_id=REPORTS_SITE_ID)
        data_sources.append(data_reports)
    if include_events:
        data_events = matomo_events(live_events_site_id=LIVE_EVENTS_SITE_ID)
        data_sources.append(data_events)
    info = pipeline.run(data_sources)
    assert_load_info(info, expected_load_packages=len(data_sources))
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
