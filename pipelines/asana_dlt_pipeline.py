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
from asana_dlt import asana_source
from typing import List

def load(resources: List[str]) -> None:
    """Execute a pipeline that will load all the resources for the given endpoints."""

    pipeline = dlt.pipeline(
        pipeline_name="asana", destination="duckdb", dataset_name="asana_data"
    )
    load_info = pipeline.run(asana_source().with_resources(*resources))
    print(load_info)


if __name__ == "__main__":
    # Add your desired resources to the list...
    resources = ["projects", "tasks", "users", "workspaces", "tags", "stories", "sections", "teams"]
    load(resources)
