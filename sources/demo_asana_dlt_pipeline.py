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
    """
    Execute a pipeline that will load all the resources for the given endpoints.
    This function initializes and runs a data loading pipeline from Asana to duckdb. The resources to load are given as arguments.
    Args:
        resources (List[str]): A list of resource names to load data from Asana. Available resources include 'workspaces',
        'projects', 'sections', 'tags', 'tasks', 'stories', 'teams', and 'users'.
    Returns:
        None: This function doesn't return any value. It prints the loading information on successful execution.
    """
    pipeline = dlt.pipeline(
        pipeline_name="asana", destination="duckdb", dataset_name="asana_data"
    )
    load_info = pipeline.run(asana_source().with_resources(*resources))
    print(load_info)


if __name__ == "__main__":
    """
    Main function to execute the data loading pipeline.
    Add your desired resources to the list and call the load function.
    """
    resources = [
        "projects",
        "tasks",
        "users",
        "workspaces",
        "tags",
        "stories",
        "sections",
        "teams",
    ]
    load(resources)
