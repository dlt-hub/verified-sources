""" This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. """

from itertools import islice
from typing import Any, Iterable, Iterator, List, Optional, Set

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .helpers import (
    JiraResponseError,
    get_paginated_data,
    nullable_key_columns,
    unseen_rows,
)
from .settings import DEFAULT_ENDPOINTS, DEFAULT_PAGE_SIZE, TEndpoint

__all__ = [
    "DEFAULT_ENDPOINTS",
    "DEFAULT_PAGE_SIZE",
    "JiraResponseError",
    "get_paginated_data",
    "jira",
    "jira_search",
]


@dlt.source(max_table_nesting=2)
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: Optional[int] = None,
) -> Iterable[DltResource]:
    """
    Jira source function that generates a list of resource functions based on endpoints.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Rows per request. Defaults to the largest page each endpoint serves.

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
            offset_path=endpoint.get("offset_path"),
            params=endpoint.get("params"),
            use_cursor_pagination=endpoint.get("use_cursor_pagination", False),
            key_field=endpoint["key_field"],
            deduplicate=endpoint.get("deduplicate", False),
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            page_size=page_size or endpoint.get("page_size", DEFAULT_PAGE_SIZE),
        )
        resources.append(res_function)

    return resources


@dlt.source(max_table_nesting=2)
def jira_search(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: Optional[int] = None,
) -> Iterable[DltResource]:
    """
    Jira search source function that generates a resource function for searching issues.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Rows per request. Defaults to the largest page the endpoint serves.

    Returns:
        Iterable[DltResource]: Resource function for searching issues.
    """
    issues_endpoint: TEndpoint = DEFAULT_ENDPOINTS["issues"]
    key_field = issues_endpoint["key_field"]

    @dlt.resource(
        write_disposition="replace",
        primary_key=issues_endpoint["primary_key"],
        columns=nullable_key_columns(issues_endpoint["primary_key"]),
    )
    def issues(
        jql_queries: List[str], page_limit: Optional[int] = None
    ) -> Iterable[TDataItem]:
        """
        Args:
            jql_queries: JQL queries to run. A query needs a filtering clause; Jira rejects
                one that only sorts.
            page_limit: Maximum number of pages to read per query, since all queries share one
                generator and `add_limit` would bound them together.
        """
        # the queries overlap, so the ids seen are shared across them
        seen: Set[Any] = set()
        for jql in jql_queries:
            params = {**issues_endpoint.get("params", {}), "jql": jql}

            pages: Iterator[TDataItem] = iter(
                get_paginated_data(
                    api_path=issues_endpoint["api_path"],
                    params=params,
                    subdomain=subdomain,
                    email=email,
                    api_token=api_token,
                    page_size=page_size
                    or issues_endpoint.get("page_size", DEFAULT_PAGE_SIZE),
                    data_path=issues_endpoint.get("data_path"),
                    use_cursor_pagination=issues_endpoint.get(
                        "use_cursor_pagination", False
                    ),
                    key_field=key_field,
                )
            )
            if page_limit is not None:
                pages = islice(pages, page_limit)
            for page in pages:
                rows = unseen_rows(page, key_field, seen)
                if rows:
                    yield rows

    return issues
