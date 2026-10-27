"""Client for the REST API of a Jira Cloud site"""

from itertools import islice
from threading import local
from typing import Any, Iterable, Iterator, List, Optional, TypeVar

from dlt.common import logger
from dlt.common.typing import DictStrAny
from dlt.sources.helpers.requests import Client, HTTPError
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.helpers.rest_client.client import PageData
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    JSONResponseCursorPaginator,
)
from dlt.sources.helpers.rest_client.typing import Hooks, HTTPMethodBasic

from .paginators import (
    JiraCursorPaginator,
    JiraOffsetPaginator,
    JiraResponseError,
    WorklogFeedPaginator,
    raise_jira_error,
    response_body,
)
from ..settings import (
    CHANGELOG_BULKFETCH_PATH,
    DEFAULT_BULK_ISSUES,
    FIELDS_PATH,
    ISSUE_BULKFETCH_PATH,
    ISSUE_COMMENTS_PATH,
    MAX_COMMENTS_PAGE_SIZE,
    MAX_WORKLOG_IDS,
    MYSELF_PATH,
    SEARCH_PATH,
    WORKLOG_LIST_PATH,
)

__all__ = ["JiraClient", "JiraResponseError", "batched"]

T = TypeVar("T")

HEADERS = {"Accept": "application/json"}
HOOKS: Hooks = {"response": [raise_jira_error]}


def batched(items: Iterable[T], size: int) -> Iterator[List[T]]:
    iterator = iter(items)
    while batch := list(islice(iterator, size)):
        yield batch


class JiraClient:
    """Calls the REST API of a Jira Cloud site with basic auth, retries HTTP 429 and 5xx.

    Errors are raised with the messages returned by Jira, e.g. for an invalid JQL query. Each
    thread gets its own `RESTClient` and session, so resources running in parallel share a client.
    """

    def __init__(self, subdomain: str, email: str, api_token: str) -> None:
        self.base_url = f"https://{subdomain}.atlassian.net/"
        self.auth = HttpBasicAuth(username=email, password=api_token)
        # sessions of this client are thread local
        self._http = Client(raise_for_status=False)
        self._local = local()
        # set when Jira rejects large bulk fetch requests
        self.split_batches = False

    @property
    def rest_client(self) -> RESTClient:
        rest_client: Optional[RESTClient] = getattr(self._local, "rest_client", None)
        if rest_client is None:
            rest_client = self._local.rest_client = RESTClient(
                base_url=self.base_url,
                headers=HEADERS,
                auth=self.auth,
                session=self._http.session,
            )
        return rest_client

    def get_json(self, path: str, params: Optional[DictStrAny] = None) -> Any:
        return response_body(self.rest_client.get(path, params=params, hooks=HOOKS))

    def post_json(self, path: str, body: DictStrAny) -> Any:
        return response_body(self.rest_client.post(path, json=body, hooks=HOOKS))

    def paginate(
        self,
        path: str,
        paginator: BasePaginator,
        data_selector: str,
        method: HTTPMethodBasic = "GET",
        params: Optional[DictStrAny] = None,
        json: Optional[DictStrAny] = None,
    ) -> Iterator[PageData[Any]]:
        """Yields pages of the data under `data_selector`, `$` for a response that is a list"""
        yield from self.rest_client.paginate(
            path,
            method=method,
            params=params,
            json=json,
            paginator=paginator,
            data_selector=data_selector,
            hooks=HOOKS,
        )

    def fields(self) -> List[DictStrAny]:
        """Returns the metadata of all issue fields"""
        fields: List[DictStrAny] = self.get_json(FIELDS_PATH)
        return fields

    def time_zone(self) -> str:
        """Returns the time zone of the user, JQL interprets datetime literals in it"""
        time_zone: Optional[str] = self.get_json(MYSELF_PATH).get("timeZone")
        if not time_zone:
            logger.warning(
                "Jira user has no time zone, JQL datetimes are rendered in UTC"
            )
            return "UTC"
        return time_zone

    def search(
        self, jql: str, fields: str, page_size: int
    ) -> Iterator[List[DictStrAny]]:
        """Yields pages of issues matching `jql` with `fields`, a comma separated list"""
        paginator = JiraCursorPaginator(SEARCH_PATH, page_size)
        params = {"jql": jql, "fields": fields}
        for page in self.paginate(SEARCH_PATH, paginator, "issues", params=params):
            if page:
                yield list(page)

    def bulk_fetch(
        self, ids: List[str], fields: List[str], batch_size: int
    ) -> List[DictStrAny]:
        """Fetches issues with `ids` in batches of `batch_size`. Issues deleted after they were
        listed are skipped.
        """
        return [
            issue
            for batch in batched(ids, batch_size)
            for issue in self._bulk_fetch(batch, fields)
        ]

    def changelogs(self, ids: List[str]) -> Iterator[List[DictStrAny]]:
        """Yields pages of changelogs of max 1000 issues with `ids`"""
        paginator = JSONResponseCursorPaginator(
            cursor_path="nextPageToken", cursor_body_path="nextPageToken"
        )
        for page in self.paginate(
            CHANGELOG_BULKFETCH_PATH,
            paginator,
            "issueChangeLogs",
            method="POST",
            json={"issueIdsOrKeys": ids},
        ):
            yield list(page)

    def comments(self, issue_id: str) -> List[DictStrAny]:
        """Returns all comments of an issue"""
        paginator = JiraOffsetPaginator(MAX_COMMENTS_PAGE_SIZE)
        path = ISSUE_COMMENTS_PATH.format(issue_id=issue_id)
        params = {"orderBy": "created"}
        return [
            comment
            for page in self.paginate(path, paginator, "comments", params=params)
            for comment in page
        ]

    def changed_worklogs(self, path: str, since: int) -> Iterator[List[DictStrAny]]:
        """Yields pages of ids of worklogs changed or deleted after `since` (UNIX timestamp in
        milliseconds)
        """
        paginator = WorklogFeedPaginator(since, MAX_WORKLOG_IDS)
        for page in self.paginate(path, paginator, "values"):
            if page:
                yield list(page)

    def worklogs(self, ids: List[int]) -> List[DictStrAny]:
        """Returns worklogs with `ids`, max 1000"""
        worklogs: List[DictStrAny] = self.post_json(WORKLOG_LIST_PATH, {"ids": ids})
        return worklogs

    def _bulk_fetch(self, ids: List[str], fields: List[str]) -> List[DictStrAny]:
        if self.split_batches and len(ids) > DEFAULT_BULK_ISSUES:
            return self.bulk_fetch(ids, fields, DEFAULT_BULK_ISSUES)
        try:
            result = self.post_json(
                ISSUE_BULKFETCH_PATH, {"issueIdsOrKeys": ids, "fields": fields}
            )
        except HTTPError as http_ex:
            if (
                http_ex.response is None
                or http_ex.response.status_code != 400
                or len(ids) <= DEFAULT_BULK_ISSUES
            ):
                raise
            logger.warning(
                f"Jira rejected bulk fetch of {len(ids)} issues, fetching {DEFAULT_BULK_ISSUES}"
                " issues per request"
            )
            self.split_batches = True
            return self.bulk_fetch(ids, fields, DEFAULT_BULK_ISSUES)
        # issues deleted after the search
        for error in result.get("issueErrors") or []:
            logger.warning(
                f"Skipping Jira issue `{error.get('id')}`: {error.get('errorMessage')}"
            )
        issues: List[DictStrAny] = result.get("issues") or []
        return issues
