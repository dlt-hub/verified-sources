from typing import List, Optional

import dlt
from jira import jira, jira_search


def load(endpoints: Optional[List[str]] = None) -> None:
    """
    Load data from specified Jira endpoints into a dataset.

    Args:
        endpoints: A list of Jira endpoints. If not provided, defaults to all resources.
    """
    if not endpoints:
        endpoints = list(jira().resources.keys())

    pipeline = dlt.pipeline(
        pipeline_name="jira_pipeline", destination="duckdb", dataset_name="jira"
    )

    load_info = pipeline.run(jira().with_resources(*endpoints))

    print(f"Load Information: {load_info}")


def load_query_data(queries: List[str]) -> None:
    """
    Load issues from specified Jira queries into a dataset.

    Args:
        queries: A list of JQL queries.
    """
    pipeline = dlt.pipeline(
        pipeline_name="jira_search_pipeline",
        destination="duckdb",
        dataset_name="jira_search",
    )

    load_info = pipeline.run(jira_search().issues(jql_queries=queries))

    print(f"Load Information: {load_info}")


if __name__ == "__main__":
    # Add your desired endpoints to the list 'endpoints'
    load(endpoints=None)

    queries = [
        "created >= -30d order by created DESC",
        'project = KAN AND status = "In Progress" order by created DESC',
    ]

    load_query_data(queries=queries)
