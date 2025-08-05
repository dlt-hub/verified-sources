"""Pipeline to load slack into duckdb."""

from typing import List

import dlt
from pendulum import datetime, now
from slack import slack_source


def load_all_resources(replies: bool = False) -> None:
    """Load all resources from slack without any selection of channels."""

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="duckdb", dataset_name="slack_data"
    )

    source = slack_source(
        page_size=1000,
        start_date=datetime(2023, 9, 1),
        end_date=datetime(2023, 9, 8),
        replies=replies,
    )

    # Uncomment the following line to load only the access_logs resource. It is not selected
    # by default because it is a resource just available on paid accounts.
    # source.access_logs.selected = True

    load_info = pipeline.run(
        source,
    )
    print(load_info)


def select_resource(selected_channels: List[str]) -> None:
    """Execute a pipeline that will load the given Slack list of channels with the selected
    channels incrementally beginning at the given start date."""

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="duckdb", dataset_name="slack_data"
    )

    source = slack_source(
        page_size=20,
        selected_channels=selected_channels,
        start_date=datetime(2023, 9, 1),
        end_date=datetime(2023, 9, 8),
    ).with_resources("channels", "1-announcements", "dlt-github-ci")

    load_info = pipeline.run(
        source,
    )
    print(load_info)


def get_users() -> None:
    """Execute a pipeline that will load Slack users list."""

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="duckdb", dataset_name="slack_data"
    )

    source = slack_source(
        page_size=20,
    ).with_resources("users")

    load_info = pipeline.run(
        source,
    )
    print(load_info)


def get_messages_and_replies_of_a_private_channel(private_channel_name: str) -> None:
    """Execute a pipeline that will load the messages and replies of a private channel."""
    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="duckdb", dataset_name="slack_data"
    )

    # Note: if you use the table_per_channel=True, the message-resource will be named after the
    # channel, so if you want the replies to a channel, e.g. "3-technical-help", you have to name
    # it like this:
    # resources = ["3-technical-help", "3-technical-help_replies"]
    source = slack_source(
        start_date=now().subtract(weeks=1),
        end_date=now(),
        selected_channels=[private_channel_name],
        include_private_channels=True,
        replies=True,
    ).with_resources(private_channel_name, f"{private_channel_name}_replies")

    load_info = pipeline.run(
        source,
    )
    print(load_info)


if __name__ == "__main__":
    # Add your desired resources to the list...
    # resources = ["access_logs", "messages", "channels", "replies"]
    # load_all_resources()

    # load all resources with replies
    # load_all_resources(replies=True)

    # select_resource(selected_channels=["dlt-github-ci"])
    # select_resource(selected_channels=["1-announcements", "dlt-github-ci"])

    # private_channel_name = "test-private-channel"
    # get_messages_and_replies_of_a_private_channel(private_channel_name=private_channel_name)

    get_users()
