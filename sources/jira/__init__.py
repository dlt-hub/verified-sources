""" This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. """

from typing import Iterable, List, Optional

import dlt
from dlt.common.typing import DictStrAny, TDataItem
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests

from .settings import DEFAULT_ENDPOINTS


@dlt.source
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    Jira source function that generates a list of resource functions based on endpoints.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
    Returns:
        Iterable[DltResource]: List of resource functions.
    """
    resources = []
    for endpoint_name, endpoint_parameters in DEFAULT_ENDPOINTS.items():
        res_function = dlt.resource(
            get_paginated_data, name=endpoint_name, write_disposition="replace"
        )(
            **endpoint_parameters, subdomain=subdomain, email=email, api_token=api_token  # type: ignore
        )
        resources.append(res_function)

    return resources


@dlt.source
def jira_search(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    Jira search source function that generates a resource function for searching issues.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
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
            )

    return issues


def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
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
        api_path: The API path for the Jira endpoint.
        data_path: Optional data path to extract from the response.
        params: Optional parameters for the API request.
    Yields:
        Iterable[TDataItem]: Yields pages of data from the API.
    """
    url = f"https://{subdomain}.atlassian.net/{api_path}"
    headers = {"Accept": "application/json"}
    auth = (email, api_token)
    start_at, max_results = 0, 50
    params = {} if params is None else params

    while True:
        params["startAt"], params["maxResults"] = start_at, max_results
        response = requests.get(url, auth=auth, headers=headers, params=params)
        response.raise_for_status()

        if data_path:
            results_page = response.json()[data_path]
        else:
            results_page = response.json()

        yield results_page

        if len(results_page) < max_results:
            break
        start_at += max_results
