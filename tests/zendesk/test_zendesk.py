import pytest
from typing import List
import dlt
from dlt.pipeline.pipeline import Pipeline
from pipelines.zendesk import zendesk_chat, zendesk_support, zendesk_talk
from tests.utils import ALL_DESTINATIONS, assert_load_info

# TODO: several endpoints are not returning data from test account. tables for those endpoints will not be created
# list expected tables and the number of columns they are supposed to have
SUPPORT_TABLES = {"ticket_fields", "tickets", "ticket_metric_events", "users", "sla_policies", "groups", "organizations", "brands", "activities", "automations", "custom_agent_roles",
                  "dynamic_content", "group_memberships", "job_status", "macros", "organization_fields", "organization_memberships", "recipient_addresses", "requests", "satisfaction_ratings",
                  "sharing_agreements", "skips", "suspended_tickets", "targets", "ticket_forms", "ticket_metrics", "triggers", "user_fields", "views", "tags"} - {'skips', 'activities', 'organization_fields', 'targets', 'suspended_tickets', 'sla_policies', 'requests', 'user_fields', 'dynamic_content', 'sharing_agreements', 'satisfaction_ratings'}
CHAT_TABLES = ["chats"]
# calls
TALK_TABLES = {"calls", "addresses", "agents_activity", "current_queue_activity", "greeting_categories", "greetings", "ivrs", "lines", "phone_numbers", "settings", "calls_incremental",
               "legs_incremental"} - {'lines', 'phone_numbers', 'calls', 'agents_activity', 'addresses', 'ivrs', 'legs_incremental', 'calls_incremental'}
# all the timezones saved in dlt state
INCREMENTAL_SAVED_KEYS = ["last_load_tickets", "last_load_ticket_metric_events", "last_load_chats", "last_load_talk_calls", "last_load_talk_legs"]
INCREMENTAL_TABLES = ["tickets", "ticket_metric_events", "chats"]  # calls_incremental and legs_incremental have no data so not added here yet


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_pivoting_tickets(destination_name: str) -> None:
    """
    Checks to see that the tickets table uses pivoting
    """

    # Run pipeline without pivoting - get columns of tickets and check that it has the expected columns
    pipeline_pivoting_1 = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_unpivot_tickets_support")
    data = zendesk_support(load_all=False, pivot_ticket_fields=False)
    info = pipeline_pivoting_1.run(data.with_resources("ticket_fields", "tickets"))
    assert_load_info(info)
    schema = pipeline_pivoting_1.default_schema
    unpivoted_tickets = schema.data_tables()[1]["columns"].keys()
    assert "custom_fields" in unpivoted_tickets
    assert "test_field" not in unpivoted_tickets

    # run pipeline with pivoting - get columns of tickets and check that it has the expected columns
    pipeline_pivoting_2 = dlt.pipeline(destination=destination_name, full_refresh=True, dataset_name="test_pivot_tickets_support")
    data2 = zendesk_support(load_all=False, pivot_ticket_fields=True)
    info2 = pipeline_pivoting_2.run(data2.with_resources("ticket_fields", "tickets"))
    assert_load_info(info2)
    schema2 = pipeline_pivoting_2.default_schema
    pivoted_tickets = schema2.data_tables()[1]["columns"].keys()
    assert "test_field" in pivoted_tickets
    assert "custom_field" not in pivoted_tickets


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incrementing(destination_name: str) -> None:
    """
    Checks that incremental loading works for ZendeskSupport
    @:param: destination_name - redshift/bigquery/postgres
    """

    data_point_count_per_table = {}
    # run pipeline
    pipeline_incremental = _create_pipeline(destination_name=destination_name, full_refresh=True, dataset_name="test_incremental", include_chat=True, include_support=True, include_talk=True)

    # check that the expected keys are saved now in dlt state
    for saved_timezone in INCREMENTAL_SAVED_KEYS:
        assert isinstance(pipeline_incremental.state["sources"]["zendesk"][saved_timezone], (float, int))

    # save the number of distinct data_points for each incremental table
    with pipeline_incremental.sql_client() as c:
        for table in INCREMENTAL_TABLES:
            # check every table has the correct name in the metadata table and the correct number of columns
            sql_query = f"SELECT * FROM {table};"
            with c.execute_query(sql_query) as cur:
                rows = list(cur.fetchall())
                data_point_count_per_table[table] = len(rows)

    # run pipeline again and check that the number of distinct data points hasn't changed
    pipeline_incremental2 = _create_pipeline(destination_name=destination_name, full_refresh=True, dataset_name="test_incremental", include_chat=True, include_support=True, include_talk=True)
    # save the number of distinct data_points for each incremental table
    with pipeline_incremental2.sql_client() as c:
        for table in INCREMENTAL_TABLES:
            # check every table has the correct name in the metadata table and the correct number of columns
            sql_query = f"SELECT * FROM {table};"
            with c.execute_query(sql_query) as cur:
                rows = list(cur.fetchall())
                assert data_point_count_per_table[table] == len(rows)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load_support(destination_name: str) -> None:
    """
    Sample pipeline run. Checks the correct amount of tables is created in the database.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # FULL PIPELINE RUN
    pipeline = _create_pipeline(destination_name=destination_name, dataset_name="test_full_load", include_support=True)
    _check_pipeline_has_tables(pipeline=pipeline, tables=SUPPORT_TABLES)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_zendesk_chat(destination_name: str) -> None:
    """
    Sample pipeline run. Checks the correct amount of tables is created in the database.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # FULL PIPELINE RUN
    pipeline = _create_pipeline(destination_name=destination_name, dataset_name="test_full_load", include_chat=True)
    # The schema should contain all listed tables
    _check_pipeline_has_tables(pipeline=pipeline, tables=CHAT_TABLES)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_zendesk_talk(destination_name: str) -> None:
    """
    Sample pipeline run. Checks the correct amount of tables is created in the database.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # FULL PIPELINE RUN
    pipeline = _create_pipeline(destination_name=destination_name, dataset_name="test_full_load", include_talk=True)
    _check_pipeline_has_tables(pipeline=pipeline, tables=TALK_TABLES)


def _create_pipeline(destination_name: str, dataset_name: str, full_refresh: bool = True, include_support: bool = False, include_chat: bool = False, include_talk: bool = False):
    """
    Helper, creates the pipelines and asserts the data is loaded correctly
    @:param: destination_name - redshift/bigquery/postgres
    @:param: full_refresh: pipeline parameter
    @:param include_support: bool that indicates whether to include zendesk support source
    @:param include_chat: bool that indicates whether to include zendesk chat source
    """
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=full_refresh, dataset_name=dataset_name)
    # gather data with sources and see which data to run or not
    source_list = []
    if include_support:
        data = zendesk_support(load_all=True)
        source_list.append(data)
    if include_chat:
        data_chat = zendesk_chat()
        source_list.append(data_chat)
    if include_talk:
        data_talk = zendesk_talk()
        source_list.append(data_talk)
    info = pipeline.run(source_list)
    assert_load_info(info=info, expected_load_packages=len(source_list))
    return pipeline


def _check_pipeline_has_tables(pipeline: Pipeline, tables: List[str]):
    """
    Helper that checks if a pipeline has all tables in the list and has the same number of proper tables as the list (child tables and dlt tables not included in this count)
    @:param pipeline: DLT pipeline
    @:param tables: list of expected table names
    """

    schema = pipeline.default_schema
    # only tables that have data are created now
    user_tables = schema.data_tables()
    # print(set(tables).difference([t["name"] for t in user_tables]))
    assert set(tables).difference([t["name"] for t in user_tables]) == set()

