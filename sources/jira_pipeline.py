import dlt
from jira import jira, jira_search
from typing import List, Optional, Dict


def load(endpoints: Optional[List[str]] = None) -> None:
    """
    Load data from specified Jira endpoints into a BigQuery dataset.

    :param endpoints: A list of Jira endpoints. If not provided, defaults to all resources.
    """
    if not endpoints:
        endpoints = jira().resources.keys()

    pipeline = dlt.pipeline(pipeline_name='strapi', destination='bigquery', dataset_name='jira')

    load_info = pipeline.run(jira().with_resources(*endpoints))

    print(f"Load Information: {load_info}")


def load_query_data(queries: List[str]) -> None:
    """
    Load data from specified Jira queries into a BigQuery dataset.

    :param queries: A list of JQL queries.
    """
    pipeline = dlt.pipeline(pipeline_name='jira_search', destination='bigquery', dataset_name='jira_search')

    load_info = pipeline.run(jira_search().issues(jql_queries=queries))

    print(f"Load Information: {load_info}")


if __name__=="__main__":
    # Add your desired endpoints to the list
    load()

    queries = [
        'created >= -30d order by created DESC',
        'created >= -30d AND assignee in (619652abc510bc006b40d007) AND project = DEV AND issuetype = Epic AND status = "In Progress" order by created DESC'
    ]

    load_query_data(queries)
