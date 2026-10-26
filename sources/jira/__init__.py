""" This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. """

from typing import Any, Dict, Iterable, List, Optional

import dlt
from dlt.common.schema.typing import TColumnSchema
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers.requests import Request, Response
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    JSONResponseCursorPaginator,
)

from .settings import DEFAULT_ENDPOINTS, DEFAULT_PAGE_SIZE, TEndpoint


class JiraResponseError(Exception):
    """Raised when a Jira endpoint returns a body this source cannot read."""


def _nullable_key_columns(primary_key: str) -> Dict[str, TColumnSchema]:
    """Hints keeping `primary_key` nullable.

    A `primary_key` hint sets `nullable: False`, and dlt then fails the whole load package
    when a single row carries a null or absent key. Jira does not guarantee the key field,
    so the hint stays nullable and a bad row lands instead of stopping the load.
    """
    return {primary_key: {"nullable": True}}


_BODY_ATTR = "_jira_parsed_body"


def _body(response: Response) -> Any:
    """Parsed response body, cached on the response.

    `Response.json()` parses the payload again on every call, and a page is read by the
    paginator, by the data path check and by dlt itself. Issue pages run to megabytes.
    """
    body = getattr(response, _BODY_ATTR, None)
    if body is None:
        body = response.json()
        setattr(response, _BODY_ATTR, body)
    return body


def _set_param(request: Request, name: str, value: Any) -> None:
    if request.params is None:
        request.params = {}
    request.params[name] = value


class JiraCursorPaginator(JSONResponseCursorPaginator):
    """Cursor pagination for `rest/api/3/search/jql`.

    An empty page carrying a `nextPageToken` is not the end of the result set, so the base
    class default of running on past an empty page is what this endpoint needs.
    """

    CURSOR_KEY = "nextPageToken"

    def __init__(self, api_path: str, page_size: int) -> None:
        super().__init__(cursor_path=self.CURSOR_KEY, cursor_param=self.CURSOR_KEY)
        self.api_path = api_path
        self.page_size = page_size

    def init_request(self, request: Request) -> None:
        super().init_request(request)
        _set_param(request, "maxResults", self.page_size)

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        body = _body(response)
        if not isinstance(body, dict):
            raise JiraResponseError(
                f"Jira endpoint {self.api_path} returned {type(body).__name__}. "
                "Cursor pagination needs an object response."
            )
        # the base class reads the same token, by parsing the body a second time
        self._next_reference = body.get(self.CURSOR_KEY) or None
        if body.get("isLast"):
            self._has_next_page = False


class JiraOffsetPaginator(BasePaginator):
    """`startAt` pagination for the page bean envelopes and for the bare user array.

    Advances by the rows a page held rather than by `maxResults`: Jira clamps the requested
    page size without reporting the clamp (100 on project search, 1000 on users), and
    stepping by the requested size would skip the rows beyond the clamp.
    """

    def __init__(self, page_size: int, offset_path: Optional[str] = None) -> None:
        super().__init__()
        self.page_size = page_size
        # key whose row count advances the offset, when the rows taken sit under a sibling key
        self.offset_path = offset_path
        self.offset = 0

    def init_request(self, request: Request) -> None:
        self.update_request(request)

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        body = _body(response)
        rows = len(data or [])
        if self.offset_path and isinstance(body, dict):
            rows = len(body.get(self.offset_path) or [])
        if not rows:
            self._has_next_page = False
            return
        self.offset += rows
        if not isinstance(body, dict):
            return
        if body.get("isLast"):
            self._has_next_page = False
            return
        total = body.get("total")
        if total is not None and self.offset >= total:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        _set_param(request, "startAt", self.offset)
        _set_param(request, "maxResults", self.page_size)


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
            write_disposition=endpoint.get("write_disposition", "replace"),
            primary_key=endpoint["primary_key"],
            columns=_nullable_key_columns(endpoint["primary_key"]),
        )(
            api_path=endpoint["api_path"],
            data_path=endpoint.get("data_path"),
            offset_path=endpoint.get("offset_path"),
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
        columns=_nullable_key_columns(issues_endpoint["primary_key"]),
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


def _check_data_path(api_path: str, data_path: str, response: Response) -> None:
    body = _body(response)
    if isinstance(body, dict) and data_path in body:
        return
    got = sorted(body) if isinstance(body, dict) else type(body).__name__
    raise JiraResponseError(
        f"Jira endpoint {api_path} returned no '{data_path}' key. Got: {got}."
    )


def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    api_path: str = "rest/api/3/search/jql",
    data_path: Optional[str] = None,
    offset_path: Optional[str] = None,
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
        offset_path: Optional response key whose row count advances `startAt`. Defaults to
            `data_path`, and differs when the rows taken sit under a sibling key.
        params: Optional parameters for the API request.
        use_cursor_pagination: If True, uses cursor-based pagination (nextPageToken)
            for the /search/jql endpoint. If False, uses offset-based pagination (startAt).
    Yields:
        Iterable[TDataItem]: Yields pages of data from the API.
    """
    client = RESTClient(
        base_url=f"https://{subdomain}.atlassian.net/",
        headers={"Accept": "application/json"},
        auth=HttpBasicAuth(username=email, password=api_token),
    )
    paginator: BasePaginator = (
        JiraCursorPaginator(api_path, page_size)
        if use_cursor_pagination
        else JiraOffsetPaginator(page_size, offset_path)
    )
    # "$" selects the whole body for the endpoints that return a bare array
    for page in client.paginate(
        api_path,
        params=dict(params or {}),
        paginator=paginator,
        data_selector=data_path or "$",
    ):
        if data_path:
            _check_data_path(api_path, data_path, page.response)
        if page:
            yield list(page)
