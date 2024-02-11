import dlt

from freshdesk import freshdesk_source, tickets


def load_endpoints() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="freshdesk_pipeline",  # Name of pipeline
        destination="duckdb",  # Target destination
        dataset_name="freshdesk_pipeline",  # Name of the dataset
    )
    # Run the pipeline with the freshdesk source
    load_info = pipeline.run(freshdesk_source())
    # print load details
    print(load_info)


def load_tickets() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="freshdesk_pipeline",  # Name of pipeline
        destination="duckdb",  # Target destination
        dataset_name="freshdesk_pipeline",  # Name of the dataset
    )
    # Run the pipeline with the tickets resource
    load_info = pipeline.run(tickets())
    # print load details
    print(load_info)


if __name__ == "__main__":
    load_endpoints()
    load_tickets()
