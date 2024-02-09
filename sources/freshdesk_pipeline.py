from typing import List

import dlt
from dlt.common.typing import TAnyDateTime
from freshdesk import freshdesk_source, tickets


def load_endpoints(endpoints: List[str] = None) -> None:

    pipeline = dlt.pipeline(
        pipeline_name="freshdesk",
        destination="bigquery",
        dataset_name="freshdesk_data",
    )
    load_info = pipeline.run(freshdesk_source(endpoints=endpoints))
    print(load_info)
    trace = pipeline.last_trace
    pipeline.run([trace], table_name="_trace")


def load_tickets(created_at: TAnyDateTime) -> None:

    pipeline = dlt.pipeline(
        pipeline_name="freshdesk",
        destination="bigquery",
        dataset_name="freshdesk_data",
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(tickets(created_at=created_at))
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # add your desired endpoints to the list
    endpoints = ["agents", "companies", "contacts", "groups", "roles", "time_entries"]
    load_endpoints(endpoints)

    # load_tickets(created_at="2022-01-01T00:00:00Z")
