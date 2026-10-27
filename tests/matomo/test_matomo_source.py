import time_machine
import pytest
from typing import List
import dlt
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny
from dlt.pipeline.pipeline import Pipeline
from sources.matomo import matomo_visits, matomo_reports
from sources.matomo.helpers.data_processing import remove_active_visits
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


QUERIES = [
    {
        "resource_name": "sample_analytics_data1",
        "methods": ["VisitsSummary.get", "API.getReportMetadata"],
        "date": "2020-01-01",
        "period": "day",
        "extra_params": {},
    },
    {
        "resource_name": "sample_analytics_data2",
        "methods": [
            "VisitsSummary.get",
            "API.getReportMetadata",
            "CustomReports.getCustomReport",
        ],
        "date": "2022-01-01",
        "period": "day",
        "extra_params": {"idCustomReport": 1},
    },
]
INCREMENTAL_END_DATES = [
    pendulum.datetime(year=2022, month=3, day=1),
    pendulum.datetime(year=2022, month=4, day=1),
    pendulum.datetime(year=2022, month=5, day=1),
]
QUERIES_START_DATE1 = [
    {
        "resource_name": "sample_analytics_data1",
        "methods": ["VisitsSummary.get"],
        "date": "2020-01-01",
        "period": "day",
        "extra_params": {},
    }
]
QUERIES_START_DATE2 = [
    {
        "resource_name": "sample_analytics_data1",
        "methods": ["VisitsSummary.get"],
        "date": "2022-01-01",
        "period": "day",
        "extra_params": {},
    }
]
ALL_TABLES_START_DATE = ["sample_analytics_data1_visits_summary_get"]
REPORTS_SITE_ID = 2
LIVE_EVENTS_SITE_ID = 2
# dict containing the name of the tables expected in the db as keys and the number of rows expected as values
ALL_TABLES_REPORTS = [
    "sample_analytics_data1_visits_summary_get",
    "sample_analytics_data1_api_get_report_metadata",
    "sample_analytics_data2_visits_summary_get",
    "sample_analytics_data2_api_get_report_metadata",
    "sample_analytics_data2_custom_reports_get_custom_report",
]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_reports(destination_name: str) -> None:
    """
    Checks to see that loading regular reports yields expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """

    pipeline = _create_pipeline(
        destination_name=destination_name,
        dataset_name="matomo_dataset",
        include_reports=True,
        include_events=False,
        queries=QUERIES,
    )
    _check_pipeline_has_tables(pipeline, ALL_TABLES_REPORTS)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_visits(destination_name: str) -> None:
    """
    Checks to see that loading live events will yield expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """

    pipeline = dlt.pipeline(
        destination=destination_name, dev_mode=True, dataset_name="matomo_dataset"
    )
    data_events = matomo_visits(
        live_events_site_id=LIVE_EVENTS_SITE_ID, get_live_event_visitors=False
    )
    info = pipeline.run(data_events)
    assert_load_info(info)
    _check_pipeline_has_tables(pipeline, ["visits"])
    counts = load_table_counts(pipeline, "visits")
    assert counts["visits"] > 20

    # load again
    data_events = matomo_visits(
        live_events_site_id=LIVE_EVENTS_SITE_ID, get_live_event_visitors=False
    )
    info = pipeline.run(data_events)
    if len(info.loads_ids) > 0:
        # we should not load anything but if we do
        assert_load_info(info)
        counts_inc = load_table_counts(pipeline, "visits")
        # we should have no more rows or maybe just 1-2
        diff_count = counts_inc["visits"] - counts["visits"]
        assert diff_count >= 0 and diff_count < 5


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_visits_with_visitors(destination_name: str) -> None:
    """
    Checks to see that loading live events will yield expected results.
    :param destination_name: Name of the db the data is loaded to.
    :returns: None
    """
    with time_machine.travel(pendulum.datetime(2022, 1, 1)):
        pipeline = dlt.pipeline(
            destination=destination_name, dev_mode=True, dataset_name="matomo_dataset"
        )
        data_events = matomo_visits(
            live_events_site_id=LIVE_EVENTS_SITE_ID,
            initial_load_past_days=365,
            get_live_event_visitors=True,
        )
        info = pipeline.run(data_events)
        assert_load_info(info)
        _check_pipeline_has_tables(pipeline, ["visits", "visitors"])
        counts = load_table_counts(pipeline, "visits", "visitors")
        assert counts["visits"] > 20
        assert counts["visitors"] > 20

    with time_machine.travel(pendulum.datetime(2020, 2, 15)):
        data_events = matomo_visits(
            live_events_site_id=LIVE_EVENTS_SITE_ID, get_live_event_visitors=True
        )
        info = pipeline.run(data_events)
        if len(info.loads_ids) > 0:
            # we should not load anything but if we do
            assert_load_info(info)
            counts_inc = load_table_counts(pipeline, "visits", "visitors")
            # we should have no more rows or maybe just 1-2
            diff_count = counts_inc["visits"] - counts["visits"]
            assert diff_count >= 0 and diff_count < 5
            diff_count = counts_inc["visitors"] - counts["visitors"]
            assert diff_count >= 0 and diff_count < 5


@pytest.mark.parametrize(
    "visits, visit_timeout_seconds, visit_max_duration_seconds, now, expected_visits",
    [
        # Test case 1: No visits should be removed
        (
            [
                {
                    "idVisit": 2,
                    "firstActionTimestamp": 2000,
                    "lastActionTimestamp": 2000,
                },
                {
                    "idVisit": 1,
                    "firstActionTimestamp": 3000,
                    "lastActionTimestamp": 3000,
                },
            ],
            1800,
            3600,
            5000,
            [
                {
                    "idVisit": 2,
                    "firstActionTimestamp": 2000,
                    "lastActionTimestamp": 2000,
                },
                {
                    "idVisit": 1,
                    "firstActionTimestamp": 3000,
                    "lastActionTimestamp": 3000,
                },
            ],
        ),
        # Test case 2: All visits should be removed
        (
            [
                {
                    "idVisit": 1,
                    "firstActionTimestamp": 1000,
                    "lastActionTimestamp": 3000,
                },
                {
                    "idVisit": 2,
                    "firstActionTimestamp": 2000,
                    "lastActionTimestamp": 4000,
                },
            ],
            1800,
            3600,
            4100,
            [],
        ),
        # Test case 3: Only the first visit should be removed, the second one exceeds the max visit time but not the visit timeout
        (
            [
                {
                    "idVisit": 2,
                    "firstActionTimestamp": 2000,
                    "lastActionTimestamp": 4000,
                },
                {
                    "idVisit": 1,
                    "firstActionTimestamp": 1000,
                    "lastActionTimestamp": 3000,
                },
            ],
            1800,
            3600,
            4700,
            [
                {
                    "idVisit": 1,
                    "firstActionTimestamp": 1000,
                    "lastActionTimestamp": 3000,
                },
            ],
        ),
        # Test case 4: A closed visit between two active visits
        (
            [
                {
                    "idVisit": 1,
                    "firstActionTimestamp": 5500,
                    "lastActionTimestamp": 7500,
                },
                {
                    "idVisit": 2,
                    "firstActionTimestamp": 2000,
                    "lastActionTimestamp": 5000,
                },
                {
                    "idVisit": 3,
                    "firstActionTimestamp": 5000,
                    "lastActionTimestamp": 7000,
                },
            ],
            1800,
            3600,
            8000,
            [],
        ),
    ],
)
def test_remove_active_visits(
    visits, visit_timeout_seconds, visit_max_duration_seconds, now, expected_visits
):
    result = remove_active_visits(
        visits, visit_timeout_seconds, visit_max_duration_seconds, now
    )
    assert result == expected_visits


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incrementing_reports(destination_name: str) -> None:
    """
    Checks that incremental loading works for reports
    @:param: destination_name - redshift/bigquery/postgres
    """
    incremental_load_counts = []
    pipeline = dlt.pipeline(
        destination=destination_name, dev_mode=True, dataset_name="matomo"
    )

    # Mock time for each iteration
    for incremental_end_date in INCREMENTAL_END_DATES:
        with time_machine.travel(incremental_end_date):
            data = matomo_reports(queries=QUERIES, site_id=REPORTS_SITE_ID)
            info = pipeline.run(data)
            assert_load_info(info)
            incremental_load_counts.append(
                load_table_counts(pipeline, *ALL_TABLES_REPORTS)
            )
            for _, resource in data.resources.items():
                assert (
                    resource.state["incremental"]["date"]["last_value"]
                    == pendulum.yesterday()
                )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_start_date(destination_name: str) -> None:
    """
    Checks that given starting dates work in matomo reports
    @:param: destination_name - redshift/bigquery/postgres
    """

    pipeline1 = _create_pipeline(
        destination_name=destination_name,
        dataset_name="matomo_start_date1",
        dev_mode=True,
        include_reports=True,
        include_events=False,
        queries=QUERIES_START_DATE1,
    )
    with pipeline1.sql_client() as client:
        result = client.execute_sql(
            f"SELECT MIN(date) as min_date FROM {pipeline1.dataset_name}.sample_analytics_data1_visits_summary_get"
        )
        min_date = list(result)[0][0]
        assert pendulum.parse(str(min_date)) >= pendulum.datetime(2020, 1, 1)
        assert pendulum.parse(str(min_date)) < pendulum.datetime(2022, 1, 1)

    pipeline2 = _create_pipeline(
        destination_name=destination_name,
        dataset_name="matomo_start_date2",
        dev_mode=True,
        include_reports=True,
        include_events=False,
        queries=QUERIES_START_DATE2,
    )

    with pipeline2.sql_client() as client:
        result = client.execute_sql(
            f"SELECT MIN(date) as min_date FROM {pipeline2.dataset_name}.sample_analytics_data1_visits_summary_get"
        )
        min_date = list(result)[0][0]
        assert pendulum.parse(str(min_date)) >= pendulum.datetime(2022, 1, 1)


def _create_pipeline(
    destination_name: str,
    dataset_name: str,
    queries: List[DictStrAny],
    dev_mode: bool = True,
    include_reports: bool = False,
    include_events: bool = False,
):
    """
    Helper, creates the pipelines and asserts the data is loaded correctly
    :param destination_name - redshift/bigquery/postgres
    :param dev_mode: pipeline parameter
    :param queries: Describes how many reports and what data should be retrieved for each report.
    :param include_reports: Bool to include the reports source
    :param include_events: Bool to include the live events source
    """
    pipeline = dlt.pipeline(
        destination=destination_name,
        dev_mode=dev_mode,
        dataset_name=dataset_name,
    )
    data_sources = []
    if include_reports:
        data_reports = matomo_reports(queries=queries, site_id=REPORTS_SITE_ID)
        data_sources.append(data_reports)
    if include_events:
        data_events = matomo_visits(live_events_site_id=LIVE_EVENTS_SITE_ID)
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
