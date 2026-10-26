""" This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. """

from typing import Dict, Iterable, List, Optional

import dlt
from dlt.common.schema.typing import TColumnSchema
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests

from .settings import DEFAULT_ENDPOINTS, DEFAULT_PAGE_SIZE, TEndpoint


def nullable_key_columns(primary_key: str) -> Dict[str, TColumnSchema]:
    """The spec marks no response field as required, so a null key must not fail the load."""
    return {primary_key: {"nullable": True}}


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
    for endpoint_name, endpoint in DEFAULT_ENDPOINTS.items():
        res_function = dlt.resource(
            get_paginated_data,
            name=endpoint_name,
            write_disposition="replace",
            primary_key=endpoint["primary_key"],
            columns=nullable_key_columns(endpoint["primary_key"]),
        )(
            api_path=endpoint["api_path"],
            data_path=endpoint.get("data_path"),
            params=endpoint.get("params"),
            use_cursor_pagination=endpoint.get("use_cursor_pagination", False),
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
    issues_endpoint: TEndpoint = DEFAULT_ENDPOINTS["issues"]

    @dlt.resource(
        write_disposition="replace",
        primary_key=issues_endpoint["primary_key"],
        columns=nullable_key_columns(issues_endpoint["primary_key"]),
    )
    def issues(jql_queries: List[str]) -> Iterable[TDataItem]:
        for jql in jql_queries:
            params = {**issues_endpoint.get("params", {}), "jql": jql}

            yield from get_paginated_data(
                api_path=issues_endpoint["api_path"],
                params=params,
                subdomain=subdomain,
                email=email,
                api_token=api_token,
                page_size=page_size,
                data_path=issues_endpoint.get("data_path"),
                use_cursor_pagination=issues_endpoint.get(
                    "use_cursor_pagination", False
                ),
            )

    return issues


def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    api_path: str = "rest/api/3/search/jql",
    data_path: Optional[str] = None,
    params: Optional[DictStrAny] = None,
    use_cursor_pagination: bool = False,
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
        use_cursor_pagination: If True, uses cursor-based pagination (nextPageToken)
            for the /search/jql endpoint. If False, uses offset-based pagination (startAt).
    Yields:
        Iterable[TDataItem]: Yields pages of data from the API.
    """
    url = f"https://{subdomain}.atlassian.net/{api_path}"
    headers = {"Accept": "application/json"}
    auth = (email, api_token)
    params = {} if params is None else dict(params)
    params["maxResults"] = page_size

    if not use_cursor_pagination:
        params["startAt"] = start_at = 0

    while True:
        response = requests.get(url, auth=auth, headers=headers, params=params)
        response.raise_for_status()
        result = response.json()

        if data_path:
            if data_path not in result:
                raise ValueError(
                    f"Jira endpoint {api_path} returned no '{data_path}' key. "
                    f"Keys returned: {sorted(result)}. The endpoint may have been removed."
                )
            results_page = result[data_path]
        else:
            results_page = result

        if results_page:
            yield results_page

        if use_cursor_pagination:
            # the token is omitted on the last page; an empty page with a token is not the end
            next_page_token = result.get("nextPageToken")
            if not next_page_token or result.get("isLast"):
                break
            params["nextPageToken"] = next_page_token
        else:
            # an empty page always ends the loop, otherwise isLast=false with no rows never advances
            if len(results_page) == 0:
                break
            start_at += len(results_page)
            if isinstance(result, dict):
                if result.get("isLast"):
                    break
                total = result.get("total")
                if total is not None and start_at >= total:
                    break
            params["startAt"] = start_at
