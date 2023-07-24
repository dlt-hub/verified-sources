"""Pipeline to load shopify data into BigQuery.
"""

import dlt
from dlt.common import pendulum
from typing import List, Tuple
from shopify_dlt import shopify_source, TAnyDateTime


def load_all_resources(resources: List[str], start_date: TAnyDateTime) -> None:
    """Execute a pipeline that will load the given Shopify resources incrementally beginning at the given start date.
    Subsequent runs will load only items updated since the previous run.
    """

    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination="duckdb", dataset_name="shopify_data"
    )
    load_info = pipeline.run(
        shopify_source(start_date=start_date).with_resources(*resources),
    )
    print(load_info)


def incremental_load_with_backloading() -> None:
    """Load past orders from Shopify in chunks of 1 week each using the start_date and end_date parameters.
    This can useful to reduce the potiential failure window when loading large amounts of historic data.
    Chunks and incremental load can also be run in parallel to speed up the initial load.
    """

    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination="duckdb", dataset_name="shopify_data"
    )

    # Load all orders from 2023-01-01 to now
    min_start_date = current_start_date = pendulum.datetime(2023, 1, 1)
    max_end_date = pendulum.now()

    # Create a list of time ranges of 1 week each, we'll use this to load the data in chunks
    ranges: List[Tuple[pendulum.DateTime, pendulum.DateTime]] = []
    while current_start_date < max_end_date:
        end_date = min(current_start_date.add(weeks=1), max_end_date)
        ranges.append((current_start_date, end_date))
        current_start_date = end_date

    # Run the pipeline for each time range created above
    for start_date, end_date in ranges:
        print(f"Load orders between {start_date} and {end_date}")
        # Create the source with start and end date set according to the current time range to filter
        # created_at_min lets us set a cutoff to exclude orders created before the initial date of (2023-01-01)
        # even if they were updated after that date
        data = shopify_source(
            start_date=start_date, end_date=end_date, created_at_min=min_start_date
        ).with_resources("orders")

        load_info = pipeline.run(data)
        print(load_info)

    # Continue loading new data incrementally starting at the end of the last range
    # created_at_min still filters out items created before 2023-01-01
    load_info = pipeline.run(
        shopify_source(
            start_date=max_end_date, created_at_min=min_start_date
        ).with_resources("orders")
    )
    print(load_info)


if __name__ == "__main__":
    # Add your desired resources to the list...
    resources = ["products", "orders", "customers"]
    load_all_resources(resources, start_date="2000-01-01")

    # incremental_load_with_backloading()
