""" This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. """

from typing import Iterable, List, Optional

import dlt
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests

from .settings import DEFAULT_ENDPOINTS, DEFAULT_PAGE_SIZE


@dlt.source(max_table_nesting=2)
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Iterable[DltResource]:
    """
    Jira source function that generates a list of resource functions based on endpoints.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Maximum number of results per page
    Returns:
        Iterable[DltResource]: List of resource functions.
    """
    resources = []
    for endpoint_name, endpoint_parameters in DEFAULT_ENDPOINTS.items():
        res_function = dlt.resource(
            get_paginated_data, name=endpoint_name, write_disposition="replace"
        )(
            **endpoint_parameters,  # type: ignore[arg-type]
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            page_size=page_size,
        )
        resources.append(res_function)

    return resources


@dlt.source(max_table_nesting=2)
def jira_search(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Iterable[DltResource]:
    """
    Jira search source function that generates a resource function for searching issues.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Maximum number of results per page
    Returns:
        Iterable[DltResource]: Resource function for searching issues.
    """

    @dlt.resource(write_disposition="replace")
    def issues(jql_queries: List[str]) -> Iterable[TDataItem]:
        api_path = "rest/api/3/search"

        for jql in jql_queries:
            params = {
                "fields": "*all",
                "expand": "fields,changelog,operations,transitions,names",
                "validateQuery": "strict",
                "jql": jql,
            }

            yield from get_paginated_data(
                api_path=api_path,
                params=params,
                subdomain=subdomain,
                email=email,
                api_token=api_token,
                page_size=page_size,
                data_path="issues",
            )

    return issues


def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    api_path: str = "rest/api/2/search",
    data_path: Optional[str] = None,
    params: Optional[DictStrAny] = None,
) -> Iterable[TDataItem]:
    """
    Function to fetch paginated data from a Jira API endpoint.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Maximum number of results per page
        api_path: The API path for the Jira endpoint.
        data_path: Optional data path to extract from the response.
        params: Optional parameters for the API request.
    Yields:
        Iterable[TDataItem]: Yields pages of data from the API.
    """
    url = f"https://{subdomain}.atlassian.net/{api_path}"
    headers = {"Accept": "application/json"}
    auth = (email, api_token)
    params = {} if params is None else params
    params["startAt"] = start_at = 0
    params["maxResults"] = page_size

    while True:
        response = requests.get(url, auth=auth, headers=headers, params=params)
        response.raise_for_status()
        result = response.json()

        if data_path:
            results_page = result.pop(data_path)
        else:
            results_page = result

        if len(results_page) == 0:
            break

        yield results_page

        # continue from next page
        start_at += len(results_page)
        params["startAt"] = start_at
