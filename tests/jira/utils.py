"""Builders, helpers and an offline mock of the Jira REST API"""

import glob
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import dlt
import requests_mock
from dlt.common.time import datetime_to_timestamp_ms, ensure_datetime

from sources.jira.helpers.client import JiraClient
from sources.jira.helpers.paginators import JiraCursorPaginator

from tests.utils import TEST_STORAGE_ROOT, assert_load_info

BASE_URL = "https://test.atlassian.net/"
CAPTURED_RESPONSES = os.path.join(os.path.dirname(__file__), "captured_responses.json")
CREDENTIALS = {"subdomain": "test", "email": "user@example.com", "api_token": "token"}
# credentials and the page size of the endpoint tests
AUTH: Dict[str, Any] = {**CREDENTIALS, "page_size": 2}
UPDATED_1 = "2024-01-15T10:30:00.123+0100"
UPDATED_2 = "2024-01-16T08:00:00.000-0800"
START = datetime(2024, 1, 15, 9, 30, 15, tzinfo=timezone.utc)
# 2024-01-01T10:00:00Z
T1 = 1704103200000
# params the Jira Cloud v3 spec does not accept on the endpoints this source calls
UNACCEPTED_PARAMS = {"validatequery", "includeinactiveusers"}
UNACCEPTED_EXPANDS = {"fields"}
# endpoint config keys that must not reach the query string
INTERNAL_KEYS = {
    "api_path",
    "data_path",
    "offset_path",
    "page_size",
    "deduplicate",
    "key_field",
    "primary_key",
}
CUSTOM = "com.atlassian.jira.plugin.system.customfieldtypes:"


def system_field(
    field_id: str, schema_type: Optional[str], items: Optional[str] = None
) -> Dict[str, Any]:
    field: Dict[str, Any] = {
        "id": field_id,
        "key": field_id,
        "name": field_id.capitalize(),
        "custom": False,
        "clauseNames": [field_id],
    }
    if schema_type:
        field["schema"] = {"type": schema_type, "system": field_id}
        if items:
            field["schema"]["items"] = items
    return field


def custom_field(
    number: int, schema_type: str, custom_type: str, items: Optional[str] = None
) -> Dict[str, Any]:
    schema: Dict[str, Any] = {
        "type": schema_type,
        "custom": custom_type,
        "customId": number,
    }
    if items:
        schema["items"] = items
    return {
        "id": f"customfield_{number}",
        "key": f"customfield_{number}",
        "name": f"Custom {number}",
        "custom": True,
        # Jira does not order clause names, the first may require quoting
        "clauseNames": [f"Custom {number}[Type]", f"cf[{number}]", f"Custom {number}"],
        "schema": schema,
    }


FIELDS = [
    system_field("summary", "string"),
    system_field("description", "string"),
    system_field("created", "datetime"),
    system_field("updated", "datetime"),
    system_field("duedate", "date"),
    system_field("status", "status"),
    system_field("project", "project"),
    system_field("assignee", "user"),
    system_field("labels", "array", "string"),
    system_field("fixVersions", "array", "version"),
    system_field("timespent", "number"),
    system_field("attachment", "array", "attachment"),
    system_field("comment", "comments-page"),
    system_field("worklog", "array", "worklog"),
    # fields without schema are loaded as text
    system_field("thumbnail", None),
    # Jira returns `parent` without schema
    system_field("parent", None),
    custom_field(10016, "number", CUSTOM + "float"),
    custom_field(10030, "option", CUSTOM + "select"),
    custom_field(10031, "array", CUSTOM + "multiuserpicker", "user"),
    custom_field(10032, "date", CUSTOM + "datepicker"),
    custom_field(10033, "datetime", CUSTOM + "datetime"),
    custom_field(10050, "any", "com.example.app:widget"),
]
# fields loaded by default, comments and worklogs have their own resources
DEFAULT_FIELD_IDS = [
    field["id"] for field in FIELDS if field["id"] not in ("comment", "worklog")
]

USER = {
    "self": "https://test.atlassian.net/rest/api/2/user?accountId=a1",
    "accountId": "a1",
    "displayName": "Ann",
    "active": True,
    "accountType": "atlassian",
    "avatarUrls": {"16x16": "https://avatar"},
    "timeZone": "Europe/Berlin",
}


def make_issue(issue_id: int, updated: str, **fields: Any) -> Dict[str, Any]:
    """Returns an issue as returned by bulk fetch, with all fields set if not passed"""
    all_fields: Dict[str, Any] = {
        "summary": f"Issue {issue_id}",
        "description": "h1. Title\n*bold*",
        "created": "2024-01-01T10:00:00.000+0100",
        "updated": updated,
        "duedate": "2024-02-01",
        "status": {
            "self": "https://test.atlassian.net/rest/api/2/status/3",
            "id": "3",
            "name": "In Progress",
            "iconUrl": "https://icon",
            "statusCategory": {"id": 4, "key": "indeterminate", "name": "In Progress"},
        },
        "project": {"id": "10000", "key": "KAN", "name": "Kanban", "avatarUrls": {}},
        "assignee": USER,
        "labels": ["backend", "urgent"],
        "fixVersions": [
            {"id": "10", "name": "1.0", "released": True, "self": "https://v"}
        ],
        "timespent": 3600,
        "attachment": [],
        "comment": {"comments": [], "total": 0},
        "worklog": {"worklogs": [], "total": 0},
        "customfield_10016": 3.5,
        "customfield_10030": {"self": "https://o", "id": "1", "value": "Red"},
        "customfield_10031": [USER],
        "customfield_10032": "2024-03-01",
        "customfield_10033": "2024-03-02T08:00:00.000-0800",
        "customfield_10050": {"anything": ["goes", 1]},
    }
    all_fields.update(fields)
    return {
        "id": str(issue_id),
        "key": f"KAN-{issue_id}",
        "self": f"https://test.atlassian.net/rest/api/2/issue/{issue_id}",
        "expand": "renderedFields",
        "fields": all_fields,
    }


def make_history(
    history_id: int, created: str, field: str = "status"
) -> Dict[str, Any]:
    """Returns a change as returned by changelog bulk fetch, with `created` in milliseconds"""
    return {
        "id": str(history_id),
        "created": datetime_to_timestamp_ms(ensure_datetime(created)),
        "author": USER,
        "items": [
            {
                "field": field,
                "fieldtype": "jira",
                "fieldId": field,
                "from": "1",
                "fromString": "To Do",
                "to": "3",
                "toString": "In Progress",
            }
        ],
    }


def make_comment(
    comment_id: int, updated: str, body: Optional[str] = None, **fields: Any
) -> Dict[str, Any]:
    """Returns a comment as returned by API v2, with the body as a string"""
    return {
        "self": f"https://test.atlassian.net/rest/api/2/issue/1/comment/{comment_id}",
        "id": str(comment_id),
        "author": USER,
        "body": body or f"comment {comment_id} with *wiki markup*",
        "updateAuthor": USER,
        "created": "2024-01-01T10:00:00.000+0100",
        "updated": updated,
        "jsdPublic": True,
        **fields,
    }


def make_worklog(worklog_id: int, issue_id: int, updated_ms: int) -> Dict[str, Any]:
    """Returns a worklog updated at `updated_ms`, the mock serves it in the worklog feeds"""
    dt = datetime.fromtimestamp(updated_ms / 1000, timezone.utc)
    # Jira format, e.g. 2024-01-01T10:00:00.000+0000
    updated = f"{dt:%Y-%m-%dT%H:%M:%S}.{dt.microsecond // 1000:03d}{dt:%z}"
    return {
        "self": f"https://test.atlassian.net/rest/api/2/issue/{issue_id}/worklog/{worklog_id}",
        "id": str(worklog_id),
        "issueId": str(issue_id),
        "author": USER,
        "updateAuthor": USER,
        "comment": "fixed the bug",
        "created": "2024-01-01T10:00:00.000+0000",
        "updated": updated,
        "started": "2024-01-01T09:00:00.000+0000",
        "timeSpent": "1h",
        "timeSpentSeconds": 3600,
        "updatedTime": updated_ms,
    }


def load_captured() -> Dict[str, Any]:
    """Loads field metadata and issues captured from the test site with `seed_test_data.py`"""
    with open(CAPTURED_RESPONSES, encoding="utf-8") as f:
        captured: Dict[str, Any] = json.load(f)
    return captured


class JiraMock:
    """Serves Jira endpoints from in-memory issues, changelogs and worklogs with `requests_mock`.

    Records query params and bodies of all requests.
    """

    def __init__(
        self,
        m: requests_mock.Mocker,
        fields: Optional[List[Dict[str, Any]]] = None,
        time_zone: Optional[str] = "UTC",
    ) -> None:
        self.issues: Dict[str, Dict[str, Any]] = {}
        # pages of issue ids returned by search, all issues in a single page if not set
        self.search_pages: Optional[List[List[str]]] = None
        # issue ids returned by search for a JQL query, overrides `search_pages`
        self.jql_results: Dict[str, List[str]] = {}
        # the last search page has a next page token and is marked with `isLast`
        self.last_page_token = False
        # bulk fetch of more issues is rejected with HTTP 400
        self.max_bulk_issues: Optional[int] = None
        self.histories: Dict[str, List[Dict[str, Any]]] = {}
        self.changelog_page_size = 100
        # comments by issue id, bulk fetch embeds the latest `embedded_comments` like Jira
        self.comments: Dict[str, List[Dict[str, Any]]] = {}
        self.embedded_comments = 20
        self.comment_page_size = 100
        self.worklogs: Dict[str, Dict[str, Any]] = {}
        self.deleted_worklogs: List[Dict[str, Any]] = []
        self.worklog_page_size = 1000

        self.searches: List[Dict[str, Any]] = []
        self.bulk_fetches: List[Dict[str, Any]] = []
        self.changelog_fetches: List[Dict[str, Any]] = []
        self.comment_fetches: List[Dict[str, Any]] = []
        self.worklog_feeds: List[Dict[str, Any]] = []
        self.worklog_lists: List[List[int]] = []

        myself = {"accountId": "me"}
        if time_zone:
            myself["timeZone"] = time_zone
        self.field_route = m.get(BASE_URL + "rest/api/2/field", json=fields or FIELDS)
        self.myself_route = m.get(BASE_URL + "rest/api/2/myself", json=myself)
        self.search_route = m.get(BASE_URL + "rest/api/2/search/jql", json=self._search)
        m.post(BASE_URL + "rest/api/2/issue/bulkfetch", json=self._bulk_fetch)
        m.post(BASE_URL + "rest/api/2/changelog/bulkfetch", json=self._changelogs)
        m.get(re.compile(r"/rest/api/2/issue/\d+/comment"), json=self._comments)
        m.get(
            re.compile(r"/rest/api/2/worklog/(updated|deleted)"),
            json=self._worklog_feed,
        )
        m.post(BASE_URL + "rest/api/2/worklog/list", json=self._worklog_list)

    def add_issues(self, *issues: Dict[str, Any]) -> None:
        for issue in issues:
            self.issues[issue["id"]] = issue

    @property
    def jqls(self) -> List[str]:
        """JQL of each search, without pages after the first"""
        return [
            search["jql"] for search in self.searches if "nextPageToken" not in search
        ]

    def _search(self, request: Any, context: Any) -> Dict[str, Any]:
        params = {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}
        self.searches.append(params)
        if params["jql"] in self.jql_results:
            ids = self.jql_results[params["jql"]]
            size = int(params["maxResults"])
            pages = [ids[i : i + size] for i in range(0, len(ids), size)]
        elif self.search_pages is not None:
            pages = self.search_pages
        else:
            pages = [list(self.issues)]
        page_idx = int(params.get("nextPageToken", 0))
        is_last = page_idx + 1 >= len(pages)
        page: Dict[str, Any] = {
            "issues": (
                [
                    self._search_result(issue_id, params["fields"])
                    for issue_id in pages[page_idx]
                ]
                if pages
                else []
            ),
            "isLast": is_last,
        }
        if not is_last or self.last_page_token:
            page["nextPageToken"] = str(page_idx + 1)
        return page

    def _search_result(self, issue_id: str, fields: str) -> Dict[str, Any]:
        """Returns an issue found by search with the requested `fields` besides the id"""
        issue_fields = self.issues.get(issue_id, {}).get("fields", {})
        return {
            "id": issue_id,
            "fields": {f: issue_fields.get(f) for f in fields.split(",") if f != "id"},
        }

    def _bulk_fetch(self, request: Any, context: Any) -> Dict[str, Any]:
        body = request.json()
        self.bulk_fetches.append(body)
        ids = body["issueIdsOrKeys"]
        if self.max_bulk_issues is not None and len(ids) > self.max_bulk_issues:
            context.status_code = 400
            return {"errorMessages": [f"Too many issues requested: {len(ids)}"]}
        issues = []
        for issue_id in ids:
            if issue_id in self.issues:
                issue = dict(self.issues[issue_id])
                issue["fields"] = {
                    k: v for k, v in issue["fields"].items() if k in body["fields"]
                }
                if "comment" in body["fields"] and issue_id in self.comments:
                    comments = self.comments[issue_id]
                    start = max(len(comments) - self.embedded_comments, 0)
                    issue["fields"]["comment"] = {
                        "comments": comments[start:],
                        "maxResults": self.embedded_comments,
                        "total": len(comments),
                        "startAt": start,
                    }
                issues.append(issue)
        errors = [
            {"id": issue_id, "errorMessage": "Issue does not exist"}
            for issue_id in ids
            if issue_id not in self.issues
        ]
        return {"issues": issues, "issueErrors": errors}

    def _changelogs(self, request: Any, context: Any) -> Dict[str, Any]:
        body = request.json()
        self.changelog_fetches.append(body)
        histories = [
            (issue_id, history)
            for issue_id in body["issueIdsOrKeys"]
            for history in self.histories.get(issue_id, [])
        ]
        start = int(body.get("nextPageToken", 0))
        end = start + self.changelog_page_size
        issue_changelogs: Dict[str, List[Dict[str, Any]]] = {}
        for issue_id, history in histories[start:end]:
            issue_changelogs.setdefault(issue_id, []).append(history)
        page: Dict[str, Any] = {
            "issueChangeLogs": [
                {"issueId": issue_id, "changeHistories": items}
                for issue_id, items in issue_changelogs.items()
            ]
        }
        if end < len(histories):
            page["nextPageToken"] = str(end)
        return page

    def _comments(self, request: Any, context: Any) -> Dict[str, Any]:
        issue_id = urlparse(request.url).path.split("/")[-2]
        params = {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}
        self.comment_fetches.append({"issueId": issue_id, **params})
        if issue_id not in self.issues:
            context.status_code = 404
            return {"errorMessages": ["Issue does not exist"]}
        comments = self.comments.get(issue_id, [])
        start = int(params.get("startAt", 0))
        size = min(int(params.get("maxResults", 50)), self.comment_page_size)
        return {
            "comments": comments[start : start + size],
            "startAt": start,
            "maxResults": size,
            "total": len(comments),
        }

    def _worklog_feed(self, request: Any, context: Any) -> Dict[str, Any]:
        params = parse_qs(urlparse(request.url).query)
        since = int(params["since"][0])
        feed = "deleted" if "/deleted" in request.url else "updated"
        self.worklog_feeds.append({"feed": feed, "since": since})
        if feed == "deleted":
            changed = self.deleted_worklogs
        else:
            changed = [
                {"worklogId": int(worklog["id"]), "updatedTime": worklog["updatedTime"]}
                for worklog in self.worklogs.values()
            ]
        # worklogs changed at `since` are returned again
        changed = sorted(
            (item for item in changed if item["updatedTime"] >= since),
            key=lambda item: item["updatedTime"],
        )
        values = changed[: self.worklog_page_size]
        return {
            "values": values,
            "since": since,
            "until": values[-1]["updatedTime"] if values else since,
            "lastPage": len(changed) <= self.worklog_page_size,
        }

    def _worklog_list(self, request: Any, context: Any) -> List[Dict[str, Any]]:
        ids = request.json()["ids"]
        self.worklog_lists.append(ids)
        return [self.worklogs[str(worklog_id)] for worklog_id in ids]


def run_to_parquet(
    name: str, data: Any, table_name: str = "issues"
) -> Tuple[dlt.Pipeline, Any]:
    """Loads `data` into parquet files on the local filesystem and reads `table_name` as an
    Arrow table
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    bucket_path = os.path.abspath(os.path.join(TEST_STORAGE_ROOT, name))
    pipeline = dlt.pipeline(
        name, destination=dlt.destinations.filesystem(bucket_path), dev_mode=True
    )
    assert_load_info(pipeline.run(data, loader_file_format="parquet"))
    files = glob.glob(f"{bucket_path}/{pipeline.dataset_name}/{table_name}/*.parquet")
    return pipeline, pa.concat_tables(pq.read_table(file) for file in files)


def to_rows(items: List[Any]) -> List[Dict[str, Any]]:
    """Converts items yielded by a resource, dicts or Arrow tables, into rows"""
    rows: List[Dict[str, Any]] = []
    for item in items:
        if hasattr(item, "to_pylist"):
            rows.extend(item.to_pylist())
        else:
            rows.append(item)
    return rows


def url_for(api_path: str) -> str:
    return BASE_URL + api_path


def serve(m: requests_mock.Mocker, api_path: str, *pages: Any) -> None:
    m.get(url_for(api_path), [{"json": page} for page in pages])


def items(pages: Iterator[Any]) -> List[Any]:
    return [item for page in pages for item in page]


def read_endpoint(api_path: str, **endpoint: Any) -> Iterator[Any]:
    """Pages of rows read from an endpoint by the client with `startAt` pagination"""
    return JiraClient(**CREDENTIALS).paginate_endpoint(
        {"api_path": api_path, "key_field": "id", "primary_key": "id", **endpoint},  # type: ignore[typeddict-item]
        endpoint.get("page_size", AUTH["page_size"]),
    )


def cursor_pages(api_path: str) -> Iterator[Any]:
    """Pages of issues read from `api_path` by the client with cursor pagination"""
    return JiraClient(**CREDENTIALS).paginate(
        api_path, JiraCursorPaginator(api_path, 2), "issues"
    )


def sent_params(m: requests_mock.Mocker) -> List[Dict[str, str]]:
    """Query string of each request. `request.qs` lowercases values, so parse the url instead."""
    return [
        {k: v[0] for k, v in parse_qs(urlparse(r.url).query).items()}
        for r in m.request_history
    ]


def assert_nullable_key(resource: Any, primary_key: str) -> None:
    key_column = resource.compute_table_schema()["columns"][primary_key]
    assert key_column["primary_key"] is True
    assert key_column["nullable"] is True
