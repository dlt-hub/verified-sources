import pytest
import dlt
from pipelines.zendesk.zendesk import zendesk
from tests.utils import ALL_DESTINATIONS, assert_load_info


# list expected tables and the number of columns they are supposed to have
BASIC_TABLES = ["ticket_metric_events", "tickets", "users", "sla_policies", "groups", "organizations", "brands" ]
EXTRA_TABLES = [
    "activities", "automations", "custom_agent_roles", "dynamic_content", "group_memberships", "job_status", "macros", "organization_fields", "organization_memberships", "recipient_addresses",
    "requests", "satisfaction_ratings", "sharing_agreements", "skips", "suspended_tickets", "targets", "ticket_forms", "ticket_metrics", "triggers", "user_fields", "views", "tags"
]
ALL_DESTINATIONS = ["postgres"]


def create_pipeline(destination_name, dataset_name, full_refresh=True, range_names=None, get_sheets=True, get_named_ranges=True):
    """
    Helper, creates the pipelines and asserts the data is loaded correctly
    """
    pipeline = dlt.pipeline(destination=destination_name, full_refresh=full_refresh, dataset_name=dataset_name)
    data = zendesk()
    info = pipeline.run(data)
    assert_load_info(info)
    return pipeline


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    Sample pipeline run. Checks the correct amount of tables is created in the database.
    @:param: destination_name - redshift/bigquery/postgres
    """

    # FULL PIPELINE RUN
    pipeline = create_pipeline(destination_name=destination_name, dataset_name="test_full_load")

    # The schema should contain all listed tables
    # ALL_TABLES is missing spreadsheet info table - table being tested here
    schema = pipeline.default_schema
    user_tables = schema.all_tables()
    assert len(user_tables) == len(BASIC_TABLES) + len(EXTRA_TABLES)
    for table in user_tables:
        assert table["name"] in BASIC_TABLES or table["name"] in EXTRA_TABLES
