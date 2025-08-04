import dlt
import pytest
from dlt.common import pendulum

from sources.slack import slack_source
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_table_per_channel(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_data",
        dev_mode=True,
    )

    # Set page size to ensure we use pagination
    source = slack_source(
        start_date=pendulum.now().subtract(weeks=1),
        end_date=pendulum.now(),
        selected_channels=["dlt-github-ci", "3-technical-help"],
    )
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    ci_table = "dlt-github-ci_message".replace("-", "_")
    help_table = "_3-technical-help_message".replace("-", "_")
    expected_tables = ["channels", ci_table, help_table]

    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["channels"] >= 15
    # Note: Message counts may vary with dynamic dates, so we check for > 0
    assert table_counts[ci_table] > 0
    assert table_counts[help_table] > 0


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_data",
        dev_mode=True,
    )

    # Set page size to ensure we use pagination
    source = slack_source(
        page_size=40,
        start_date=pendulum.now().subtract(weeks=1),
        end_date=pendulum.now(),
        selected_channels=["dlt-github-ci", "1-announcements"],
        table_per_channel=False,
    )
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    expected_tables = ["channels", "messages"]

    assert set(table_counts.keys()) >= set(expected_tables)
    assert "replies" not in table_names
    assert table_counts["channels"] >= 15
    # Note: Message counts may vary with dynamic dates, so we check for > 0
    assert table_counts["messages"] > 0


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_replies(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_data",
        dev_mode=True,
    )

    # Set page size to ensure we use pagination
    source = slack_source(
        start_date=pendulum.now().subtract(weeks=1),
        end_date=pendulum.now(),
        selected_channels=["3-technical-help"],
        replies=True,
        table_per_channel=False,
    )
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert "replies" in table_names
    # Note: Reply counts may vary with dynamic dates, so we check for > 0
    assert table_counts["replies"] > 0


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("table_per_channel", [True, False])
def test_with_merge_disposition(destination_name: str, table_per_channel: bool) -> None:
    """Checks the case if we run pipeline two times in a row with specified start and end dates.
    In this case we don't want messages to be duplicated, therefore we source automatically changes write disposition to merge.
    """
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_data",
        dev_mode=True,
    )

    # Set page size to ensure we use pagination
    source = slack_source(
        start_date=pendulum.now().subtract(weeks=4),
        end_date=pendulum.now().subtract(weeks=1),
        selected_channels=["1-announcements"],
        replies=True,
        table_per_channel=table_per_channel,
    )
    pipeline.run(source)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    current_table_counts = load_table_counts(pipeline, *table_names)
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert all(
        table_counts[table_name] == current_table_counts[table_name]
        for table_name in table_names
        if table_name != "users"
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_users(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_user_data",
        dev_mode=True,
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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_private_channels(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination=destination_name,
        dataset_name="slack_data",
        dev_mode=True,
    )
    PRIVATE_CHANNEL_NAME = "test-private-channel"
    source = slack_source(
        start_date=pendulum.now().subtract(weeks=1),
        end_date=pendulum.now(),
        selected_channels=[PRIVATE_CHANNEL_NAME],
        include_private_channels=True,
        replies=True,
    ).with_resources(PRIVATE_CHANNEL_NAME, f"{PRIVATE_CHANNEL_NAME}_replies")
    load_info = pipeline.run(source)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]

    expected_message_table_name = f"{PRIVATE_CHANNEL_NAME}_message".replace("-", "_")
    expected_replies_table_name = f"{PRIVATE_CHANNEL_NAME}_replies_message".replace(
        "-", "_"
    )

    assert expected_message_table_name in table_names
    assert expected_replies_table_name in table_names
