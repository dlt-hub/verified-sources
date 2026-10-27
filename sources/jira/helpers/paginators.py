"""Paginators and response handling of the Jira REST API"""

from typing import Any, List, Optional
from urllib.parse import urlparse

from dlt.sources.helpers.requests import HTTPError, Request, Response
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    JSONResponseCursorPaginator,
)

_BODY_ATTR = "_jira_parsed_body"


class JiraResponseError(HTTPError):
    """Raised for an HTTP error with the messages Jira returned and for a body this source
    cannot read
    """


def response_body(response: Response) -> Any:
    """Parsed response body, cached on the response.

    `Response.json()` parses the payload again on every call, and issue pages run to megabytes.
    dlt parses each page once to apply the data selector, and this cache holds the source to one
    parse on top of that.
    """
    body = getattr(response, _BODY_ATTR, None)
    if body is None:
        body = response.json()
        setattr(response, _BODY_ATTR, body)
    return body


def raise_jira_error(response: Response, *args: Any, **kwargs: Any) -> None:
    """Response hook replacing `raise_for_status`, so the messages Jira sends reach the user.

    An error body carries `errorMessages` and `errors`, and `raise_for_status` reports only the
    status code. `rest/api/3/search/jql` rejects a JQL query with no filtering clause that way.
    """
    if response.status_code < 400:
        return
    body = None
    try:
        body = response_body(response)
    except ValueError:
        pass
    messages: List[str] = []
    if isinstance(body, dict):
        messages.extend(body.get("errorMessages") or [])
        messages.extend(f"{k}: {v}" for k, v in (body.get("errors") or {}).items())
    if not messages:
        response.raise_for_status()
        return
    raise JiraResponseError(
        f"Jira endpoint {urlparse(response.url).path.lstrip('/')} answered"
        f" {response.status_code}: {' '.join(messages)}",
        response=response,
    )


def _set_param(request: Request, name: str, value: Any) -> None:
    if request.params is None:
        request.params = {}
    request.params[name] = value


class JiraCursorPaginator(JSONResponseCursorPaginator):
    """Cursor pagination for `search/jql`.

    An empty page carrying a `nextPageToken` is not the end of the result set, so the base
    class default of running on past an empty page is what this endpoint needs. The base class
    stops when the token is absent. This adds a second stop on `isLast` and the `maxResults`
    parameter.
    """

    CURSOR_KEY = "nextPageToken"

    def __init__(self, api_path: str, page_size: Optional[int] = None) -> None:
        super().__init__(cursor_path=self.CURSOR_KEY, cursor_param=self.CURSOR_KEY)
        self.api_path = api_path
        self.page_size = page_size

    def init_request(self, request: Request) -> None:
        super().init_request(request)
        if self.page_size:
            _set_param(request, "maxResults", self.page_size)

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        body = response_body(response)
        if not isinstance(body, dict):
            raise JiraResponseError(
                f"Jira endpoint {self.api_path} returned {type(body).__name__}. "
                "Cursor pagination needs an object response.",
                response=response,
            )
        # the base class reads the same token, by parsing the body a second time
        self._next_reference = body.get(self.CURSOR_KEY) or None
        if body.get("isLast"):
            self._has_next_page = False


class JiraOffsetPaginator(BasePaginator):
    """`startAt` pagination for the page bean envelopes and for the bare user array.

    Advances by the rows a page held, because Jira clamps `maxResults` without reporting it.
    """

    # the `nextPage` link is no alternative: workflows/search returns an internal Atlassian host

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
        body = response_body(response)
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


class WorklogFeedPaginator(BasePaginator):
    """Pages through the feeds of changed and deleted worklogs. A page ends at the youngest
    change it holds, the next page starts there.
    """

    def __init__(self, since: int, max_page_size: int) -> None:
        super().__init__()
        self.since = since
        self.max_page_size = max_page_size

    def init_request(self, request: Request) -> None:
        self.update_request(request)

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        body = response_body(response)
        if body.get("lastPage", True):
            self._has_next_page = False
            return
        until: int = body["until"]
        if until <= self.since:
            raise RuntimeError(
                f"Cannot page Jira worklogs changed at {self.since}: more than"
                f" {self.max_page_size} worklogs share the same timestamp"
            )
        self.since = until

    def update_request(self, request: Request) -> None:
        _set_param(request, "since", self.since)
