#!/usr/bin/env python3
"""Pipeline to load Salesforce data."""
import dlt
from salesforce import salesforce_source


def load(destination: str) -> None:
    """Execute a pipeline from Salesforce."""

    pipeline = dlt.pipeline(
        pipeline_name="salesforce", destination=destination, dataset_name="salesforce"
    )
    source = salesforce_source()

    # Add schema hints as needed...
    source.schema.merge_hints({"not_null": ["id"]})

    # Execute the pipeline
    load_info = pipeline.run(source)

    # Print the load info
    print(load_info)


if __name__ == "__main__":
    load(destination="bigquery")
