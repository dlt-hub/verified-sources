from typing import List

import dlt

from freshdesk import freshdesk_source


def load_endpoints(selected_endpoints: List[str] = None) -> None:
    """
    This demo script demonstrates the use of resources with incremental loading,
    based on the append mode to load data from custom or default endpoints in 'settings.py'.

    Args:
        selected_endpoints: A list of endpoint names to retrieve data from. Defaults to most
                   popular Freshdesk endpoints.
    """

    pipeline = dlt.pipeline(
        pipeline_name="freshdesk_pipeline",
        destination="duckdb",
        dataset_name="freshdesk_data",
    )

    # Run the pipeline
    load_info = pipeline.run(freshdesk_source(endpoints=selected_endpoints))

    # Print the pipeline run information
    print(load_info)


if __name__ == "__main__":
    # To load all the default endpoints
    load_endpoints()

    # To load data from selective endpoints
    custom_endpoints = [
        "agents",
        "companies",
    ]  # Renamed from 'endpoints' to 'custom_endpoints'
    load_endpoints(custom_endpoints)  # Pass 'custom_endpoints' instead of 'endpoints'
