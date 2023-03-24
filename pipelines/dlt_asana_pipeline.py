"""Pipeline to load Asana data into BigQuery.

Available resources:
    workspaces
    projects
    sections
    tags
    tasks
    stories
    teams
    users
"""
import dlt
from dlt_asana import asana_source


def load(*resources: str) -> None:
    """Execute a pipeline that will load all the resources for the given endpoints."""

    pipeline = dlt.pipeline(
        pipeline_name="asana", destination="bigquery", dataset_name="asana"
    )
    load_info = pipeline.run(asana_source().with_resources(*resources))
    print(load_info)


if __name__ == "__main__":
    # Add your desired resources to the list...
    resources = ["projects", "tasks", "users", "workspaces"]
    load(resources)
