import pytest
import dlt
from pipelines.zendesk.zendesk import zendesk
from tests.utils import ALL_DESTINATIONS, assert_load_info


# list expected tables and the number of columns they are supposed to have
ALL_TABLES = ["tickets", "organizations", "users"]
COL_NUMS = [11, 6, 7]


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
    assert len(user_tables) == len(ALL_TABLES)
    for table in user_tables:
        assert table["name"] in ALL_TABLES

    # all tables have correct info in them
    ticket_table = schema.get_table(ALL_TABLES[0])
    org_table = schema.get_table(ALL_TABLES[1])
    usr_table = schema.get_table(ALL_TABLES[2])

    # check all columns
    assert len(ticket_table["columns"]) == COL_NUMS[0]
    assert len(org_table["columns"]) == COL_NUMS[1]
    assert len(usr_table["columns"]) == COL_NUMS[2]
