from typing import List, Optional

import dlt
from jira import jira, jira_search


def load(endpoints: Optional[List[str]] = None, limit: Optional[int] = None) -> None:
    """
    Load data from specified Jira endpoints into a dataset.

    Args:
        endpoints: A list of Jira endpoints. If not provided, defaults to all resources.
        limit: Maximum number of pages per resource.
    """
    pipeline = dlt.pipeline(
        pipeline_name="jira_pipeline", destination="duckdb", dataset_name="jira"
    )

    source = jira()
    if endpoints:
        source = source.with_resources(*endpoints)
    if limit is not None:
        source.add_limit(limit)

    load_info = pipeline.run(source)

    print(f"Load Information: {load_info}")


def load_query_data(queries: List[str], limit: Optional[int] = None) -> None:
    """
    Load issues from specified Jira queries into a dataset.

    Args:
        queries: A list of JQL queries.
        limit: Maximum number of pages across all queries.
    """
    pipeline = dlt.pipeline(
        pipeline_name="jira_search_pipeline",
        destination="duckdb",
        dataset_name="jira_search",
    )

    source = jira_search()
    source.issues.bind(jql_queries=queries)
    if limit is not None:
        source.add_limit(limit)

    load_info = pipeline.run(source)

    print(f"Load Information: {load_info}")


if __name__ == "__main__":
    # issues load in the query run below
    load(endpoints=["users", "projects", "workflows"], limit=2)

    queries = [
        "created >= -30d order by created DESC",
        'status = "In Progress" order by created DESC',
    ]

    load_query_data(queries=queries, limit=2)
