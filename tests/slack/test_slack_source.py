import dlt
import pytest
from pendulum import datetime

from sources.slack import slack_source
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_data",
        full_refresh=True,
    )

    # Set page size to ensure we use pagination
    source = slack_source(
        page_size=20,
        start_date=datetime(2023, 9, 1),
        end_date=datetime(2023, 9, 8),
        selected_channels=["dlt-github-ci", "1-announcements"],
    )
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    expected_tables = ["channels", "messages"]

    assert set(table_counts.keys()) >= set(expected_tables)
    assert "replies" not in table_names
    assert table_counts["channels"] >= 15
    assert table_counts["messages"] == 26


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_replies(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_data",
        full_refresh=True,
    )

    # Set page size to ensure we use pagination
    source = slack_source(
        page_size=20,
        start_date=datetime(2023, 9, 1),
        end_date=datetime(2023, 9, 8),
        selected_channels=["dlt-github-ci", "1-announcements"],
        replies=True,
    )
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert "replies" in table_names
    assert table_counts["replies"] >= 10


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_users(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_user_data",
        full_refresh=True,
    )

    # Selected just one channel to avoid loading all channels
    source = slack_source(
        selected_channels=["1-announcements"],
    )
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # check if the users table was loaded
    expected_tables = ["users"]
    print(table_counts.keys())
    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["users"] >= 300  # The number of users can increase over time
