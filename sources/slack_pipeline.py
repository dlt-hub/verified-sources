"""Pipeline to load shopify data into BigQuery.
"""

from typing import List

import dlt
from pendulum import datetime
from slack import slack_source


def load_all_resources() -> None:
    """Execute a pipeline that will load the given Shopify resources incrementally beginning at the given start date.
    Subsequent runs will load only items updated since the previous run.
    """

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="duckdb", dataset_name="slack_data"
    )

    source = slack_source(
        page_size=1000, start_date=datetime(2023, 9, 1), end_date=datetime(2023, 9, 8)
    )

    # Uncomment the following line to load only the access_logs resource. It is not selectes
    # by default because it is a resource just available on paid accounts.
    # source.access_logs.selected = True

    load_info = pipeline.run(
        source,
    )
    print(load_info)


def select_resource(selected_channels: List[str]) -> None:
    """Execute a pipeline that will load the given Shopify resources incrementally beginning at the given start date.
    Subsequent runs will load only items updated since the previous run.
    """

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="duckdb", dataset_name="slack_data"
    )

    source = slack_source(
        page_size=20,
        selected_channels=selected_channels,
        start_date=datetime(2023, 9, 1),
        end_date=datetime(2023, 9, 8),
    )

    load_info = pipeline.run(
        source,
    )
    print(load_info)


if __name__ == "__main__":
    # Add your desired resources to the list...
    # resources = ["access_logs", "conversations", "conversations_history"]

    # load_all_resources()
    # select_resource(selected_channels=["dlt-github-ci"])

    select_resource(selected_channels=["1-announcements", "dlt-github-ci"])
