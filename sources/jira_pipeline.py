from typing import List, Optional

import dlt
from jira import drop_keys, issue_comments, jira, jira_issues


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
    # skip REST links and avatar and icon urls, pass key names to `drop_keys` to skip others
    for resource in source.resources.values():
        resource.add_map(drop_keys())

    load_info = pipeline.run(source)

    print(f"Load Information: {load_info}")


def load_issues_with_jql(jql: str, with_comments: bool = True) -> None:
    """
    Incrementally load issues matching a JQL query, merging issues updated since the last run.

    Args:
        jql: JQL query without ORDER BY.
        with_comments: Also load all comments of the issues.
    """
    pipeline = dlt.pipeline(
        pipeline_name="jira_issues_pipeline",
        destination="duckdb",
        dataset_name="jira_issues",
    )

    issues = jira_issues(jql=jql).add_map(drop_keys())
    # pass `issues | issue_comments` alone to load comments without issues
    data = (
        [issues, (issues | issue_comments).add_map(drop_keys())]
        if with_comments
        else issues
    )
    load_info = pipeline.run(data)

    print(f"Load Information: {load_info}")


if __name__ == "__main__":
    # issues load in the run below
    load(endpoints=["users", "projects", "workflows", "workflow_statuses"], limit=2)

    load_issues_with_jql("updated >= -30d")
