"""Pipeline to load shopify data into BigQuery.
"""

import dlt
from typing import List
from shopify_dlt import shopify_source


def load(resources: List[str], start_date: str) -> None:
    """Execute a pipeline that will load all the resources for the given endpoints."""

    pipeline = dlt.pipeline(
        pipeline_name="shopify", destination="duckdb", dataset_name="shopify_data"
    )
    load_info = pipeline.run(
        shopify_source(start_date=start_date).with_resources(*resources)
    )
    print(load_info)


if __name__ == "__main__":
    # Add your desired resources to the list...
    resources = ["products", "orders", "customers"]
    load(resources, start_date="2000-01-01")
