from typing import Any, Dict, Iterator, List

import pytest

from sources.jira import get_paginated_data, jira, jira_search
from sources.jira.settings import DEFAULT_ENDPOINTS

# params the Jira Cloud v3 spec rejects on the endpoints this source calls
REJECTED_PARAMS = {"validateQuery", "includeInactiveUsers", "primary_key", "columns"}
REJECTED_EXPANDS = {"fields"}

AUTH = dict(subdomain="example", email="e@example.com", api_token="token", page_size=2)


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> Any:
        return self._payload


class StubGet:
    """Serves `pages` in order, then `exhausted`, and records the params of each request."""

    def __init__(self, pages: List[Any], exhausted: Any = None) -> None:
        self.pages = pages
        self.exhausted = exhausted
        self.sent: List[Dict[str, Any]] = []

    def __call__(self, url: str, **kwargs: Any) -> FakeResponse:
        self.sent.append(dict(kwargs["params"]))
        if len(self.sent) <= len(self.pages):
            return FakeResponse(self.pages[len(self.sent) - 1])
        return FakeResponse(self.exhausted)


def install(monkeypatch: pytest.MonkeyPatch, stub: StubGet) -> StubGet:
    monkeypatch.setattr("sources.jira.requests.get", stub)
    return stub


def items(pages: Iterator[Any]) -> List[Any]:
    return [item for page in pages for item in page]


def test_cursor_pagination_continues_past_empty_page_with_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # SearchAndReconcileResults: nextPageToken is omitted on the last page only
    stub = install(
        monkeypatch,
        StubGet(
            [
                {"issues": [{"id": "1"}], "nextPageToken": "t1"},
                {"issues": [], "nextPageToken": "t2"},
                {"issues": [{"id": "2"}]},
            ]
        ),
    )
    data = items(
        get_paginated_data(
            api_path="rest/api/3/search/jql",
            data_path="issues",
            use_cursor_pagination=True,
            **AUTH,
        )
    )
    assert [i["id"] for i in data] == ["1", "2"]
    assert [p.get("nextPageToken") for p in stub.sent] == [None, "t1", "t2"]


def test_offset_pagination_stops_on_is_last(monkeypatch: pytest.MonkeyPatch) -> None:
    # PageBeanProject: one complete page must cost one request
    stub = install(
        monkeypatch,
        StubGet(
            [{"values": [{"id": "p1"}], "total": 1, "isLast": True}], {"values": []}
        ),
    )
    data = items(
        get_paginated_data(
            api_path="rest/api/3/project/search", data_path="values", **AUTH
        )
    )
    assert [i["id"] for i in data] == ["p1"]
    assert len(stub.sent) == 1


def test_offset_pagination_follows_is_last_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # WorkflowSearchResponse
    stub = install(
        monkeypatch,
        StubGet(
            [
                {"values": [{"id": "w1"}, {"id": "w2"}], "total": 3, "isLast": False},
                {"values": [{"id": "w3"}], "total": 3, "isLast": True},
            ],
            {"values": []},
        ),
    )
    data = items(
        get_paginated_data(
            api_path="rest/api/3/workflows/search", data_path="values", **AUTH
        )
    )
    assert [i["id"] for i in data] == ["w1", "w2", "w3"]
    assert [p["startAt"] for p in stub.sent] == [0, 2]


def test_offset_pagination_stops_on_empty_page_with_is_last_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = {"values": [], "total": 5, "isLast": False}
    stub = install(monkeypatch, StubGet([page], page))
    data = items(
        get_paginated_data(
            api_path="rest/api/3/workflows/search", data_path="values", **AUTH
        )
    )
    assert data == []
    assert len(stub.sent) == 1


def test_bare_array_endpoint_stops_on_empty_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # /rest/api/3/users returns a list of User with no envelope
    stub = install(monkeypatch, StubGet([[{"accountId": "a"}, {"accountId": "b"}], []]))
    data = items(get_paginated_data(api_path="rest/api/3/users", **AUTH))
    assert [i["accountId"] for i in data] == ["a", "b"]
    assert [p["startAt"] for p in stub.sent] == [0, 2]


def test_missing_data_path_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    install(
        monkeypatch,
        StubGet([{"errorMessages": ["The requested endpoint has been removed."]}]),
    )
    with pytest.raises(
        ValueError, match="rest/api/3/workflow/search.*'values'.*errorMessages"
    ):
        list(
            get_paginated_data(
                api_path="rest/api/3/workflow/search", data_path="values", **AUTH
            )
        )


def assert_params_accepted(params: Dict[str, Any]) -> None:
    assert not REJECTED_PARAMS & set(params)
    assert not REJECTED_EXPANDS & set(params.get("expand", "").split(","))
    assert params["maxResults"] == AUTH["page_size"]


@pytest.mark.parametrize("endpoint_name", list(DEFAULT_ENDPOINTS))
def test_jira_resource_request_and_hints(
    endpoint_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty_page = [] if endpoint_name == "users" else {"issues": [], "values": []}
    stub = install(monkeypatch, StubGet([], exhausted=empty_page))
    resource = jira(**AUTH).resources[endpoint_name]
    assert list(resource) == []
    assert_params_accepted(stub.sent[0])

    primary_key = DEFAULT_ENDPOINTS[endpoint_name]["primary_key"]
    key_column = resource.compute_table_schema()["columns"][primary_key]
    assert key_column["primary_key"] is True
    assert key_column["nullable"] is True


def test_jira_search_request_and_hints(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = install(monkeypatch, StubGet([], exhausted={"issues": []}))
    resource = jira_search(**AUTH).issues(jql_queries=["project = X", "project = Y"])
    assert list(resource) == []
    assert [p["jql"] for p in stub.sent] == ["project = X", "project = Y"]
    for params in stub.sent:
        assert_params_accepted(params)

    key_column = resource.compute_table_schema()["columns"]["id"]
    assert key_column["primary_key"] is True
    assert key_column["nullable"] is True
