import pytest
import dlt
from pipelines.zendesk.zendesk import zendesk, zendesk_chat
from tests.utils import ALL_DESTINATIONS, assert_load_info


# list expected tables and the number of columns they are supposed to have
BASIC_TABLES = ["ticket_fields", "tickets", "ticket_metric_events", "users", "sla_policies", "groups", "organizations", "brands"]
EXTRA_TABLES = [
    "activities", "automations", "custom_agent_roles", "dynamic_content", "group_memberships", "job_status", "macros", "organization_fields", "organization_memberships", "recipient_addresses",
    "requests", "satisfaction_ratings", "sharing_agreements", "skips", "suspended_tickets", "targets", "ticket_forms", "ticket_metrics", "triggers", "user_fields", "views", "tags"
]
ALL_TABLES = ["ticket_fields", "tickets", "ticket_metric_events", "users", "sla_policies", "groups", "organizations", "brands", "activities", "automations", "custom_agent_roles", "dynamic_content",
              "group_memberships", "job_status", "macros", "organization_fields", "organization_memberships", "recipient_addresses", "requests", "satisfaction_ratings", "sharing_agreements", "skips",
              "suspended_tickets", "targets", "ticket_forms", "ticket_metrics", "triggers", "user_fields", "views", "tags"]
CHAT_TABLES = ["chats"]
ALL_DESTINATIONS = ["postgres"]


def create_pipeline(destination_name, dataset_name, full_refresh=True, include_support: bool = True, include_chat: bool = False):
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
        data = zendesk(load_all=True)
        source_list.append(data)
    if include_chat:
        data_chat = zendesk_chat(credentials=dlt.secrets["sources.zendesk_chat.credentials"].value)
        source_list.append(data_chat)
    info = pipeline.run(source_list)
    assert_load_info(info, expected_load_packages=len(source_list))
    return pipeline


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load_support(destination_name: str) -> None:
    """
    Sample pipeline run. Checks the correct amount of tables is created in the database.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # FULL PIPELINE RUN
    pipeline = create_pipeline(destination_name=destination_name, dataset_name="test_full_load", include_support=True, include_chat=False)

    # The schema should contain all listed tables
    schema = pipeline.default_schema
    user_tables = schema.all_tables()
    num_proper_tables = 0
    for table in user_tables:
        table_name = table["name"]
        if not ("__" in table_name):
            assert table_name in ALL_TABLES
            num_proper_tables += 1
    assert num_proper_tables == len(ALL_TABLES)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_zendesk_chat(destination_name: str) -> None:
    """
    Sample pipeline run. Checks the correct amount of tables is created in the database.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # FULL PIPELINE RUN
    pipeline = create_pipeline(destination_name=destination_name, dataset_name="test_full_load", include_chat=True, include_support=False)

    # The schema should contain all listed tables
    schema = pipeline.default_schema
    user_tables = schema.all_tables()
    num_proper_tables = 0
    for table in user_tables:
        table_name = table["name"]
        if not ("__" in table_name):
            assert table_name in CHAT_TABLES
            num_proper_tables += 1
    assert num_proper_tables == len(CHAT_TABLES)

