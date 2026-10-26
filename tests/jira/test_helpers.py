from base64 import b64decode
from typing import Any, Dict, Iterator, List
from urllib.parse import parse_qs, urlparse

import dlt
import pytest
import requests_mock
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.sources.helpers.requests import Response

from sources.jira import JiraResponseError, get_paginated_data, jira, jira_search
from sources.jira.settings import DEFAULT_ENDPOINTS

from tests.utils import assert_load_info, load_table_counts

# params the Jira Cloud v3 spec does not accept on the endpoints this source calls
UNACCEPTED_PARAMS = {"validatequery", "includeinactiveusers"}
UNACCEPTED_EXPANDS = {"fields"}
# endpoint config keys that must not reach the query string
INTERNAL_KEYS = {
    "api_path",
    "data_path",
    "offset_path",
    "use_cursor_pagination",
    "deduplicate_on",
    "primary_key",
}

AUTH = dict(subdomain="example", email="e@example.com", api_token="token", page_size=2)
BASE_URL = f"https://{AUTH['subdomain']}.atlassian.net/"


def url_for(api_path: str) -> str:
    return BASE_URL + api_path


def serve(m: requests_mock.Mocker, api_path: str, *pages: Any) -> None:
    m.get(url_for(api_path), [{"json": page} for page in pages])


def items(pages: Iterator[Any]) -> List[Any]:
    return [item for page in pages for item in page]


def sent_params(m: requests_mock.Mocker) -> List[Dict[str, str]]:
    """Query string of each request. `request.qs` lowercases values, so parse the url instead."""
    return [
        {k: v[0] for k, v in parse_qs(urlparse(r.url).query).items()}
        for r in m.request_history
    ]


def test_cursor_pagination_continues_past_empty_page_with_token() -> None:
    # SearchAndReconcileResults: nextPageToken is omitted on the last page only
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/search/jql",
            {"issues": [{"id": "1"}], "nextPageToken": "t1"},
            {"issues": [], "nextPageToken": "t2"},
            {"issues": [{"id": "2"}]},
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
        assert [p.get("nextPageToken") for p in sent_params(m)] == [None, "t1", "t2"]


def test_cursor_pagination_stops_on_is_last_with_token() -> None:
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/search/jql",
            {"issues": [{"id": "1"}], "nextPageToken": "t1", "isLast": True},
        )
        data = items(
            get_paginated_data(
                api_path="rest/api/3/search/jql",
                data_path="issues",
                use_cursor_pagination=True,
                **AUTH,
            )
        )
        assert [i["id"] for i in data] == ["1"]
        assert m.call_count == 1


def test_offset_pagination_stops_on_is_last() -> None:
    # PageBeanProject: one complete page must cost one request
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/project/search",
            {"values": [{"id": "p1"}], "total": 1, "isLast": True},
            {"values": []},
        )
        data = items(
            get_paginated_data(
                api_path="rest/api/3/project/search", data_path="values", **AUTH
            )
        )
        assert [i["id"] for i in data] == ["p1"]
        assert m.call_count == 1


def test_offset_pagination_follows_is_last_false() -> None:
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/workflows/search",
            {"values": [{"id": "w1"}, {"id": "w2"}], "total": 3, "isLast": False},
            {"values": [{"id": "w3"}], "total": 3, "isLast": True},
            {"values": []},
        )
        data = items(
            get_paginated_data(
                api_path="rest/api/3/workflows/search", data_path="values", **AUTH
            )
        )
        assert [i["id"] for i in data] == ["w1", "w2", "w3"]
        assert [p["startAt"] for p in sent_params(m)] == ["0", "2"]


def test_offset_pagination_stops_on_total_without_is_last() -> None:
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/workflows/search",
            {"values": [{"id": "w1"}, {"id": "w2"}], "total": 3},
            {"values": [{"id": "w3"}], "total": 3},
            {"values": []},
        )
        data = items(
            get_paginated_data(
                api_path="rest/api/3/workflows/search", data_path="values", **AUTH
            )
        )
        assert [i["id"] for i in data] == ["w1", "w2", "w3"]
        assert m.call_count == 2


def test_offset_pagination_advances_by_rows_when_jira_clamps_the_page_size() -> None:
    # project search clamps maxResults to 100, stepping by the requested size would skip rows
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/project/search",
            {"values": [{"id": "p1"}], "total": 2},
            {"values": [{"id": "p2"}], "total": 2},
        )
        data = items(
            get_paginated_data(
                api_path="rest/api/3/project/search",
                data_path="values",
                **{**AUTH, "page_size": 500},
            )
        )
        assert [i["id"] for i in data] == ["p1", "p2"]
        assert [p["startAt"] for p in sent_params(m)] == ["0", "1"]


def test_offset_pagination_stops_on_empty_page_with_is_last_false() -> None:
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/workflows/search",
            {"values": [], "total": 5, "isLast": False},
        )
        data = items(
            get_paginated_data(
                api_path="rest/api/3/workflows/search", data_path="values", **AUTH
            )
        )
        assert data == []
        assert m.call_count == 1


def test_bare_array_endpoint_stops_on_empty_page() -> None:
    # /rest/api/3/users returns a list of User with no envelope
    with requests_mock.Mocker() as m:
        serve(m, "rest/api/3/users", [{"accountId": "a"}, {"accountId": "b"}], [])
        data = items(get_paginated_data(api_path="rest/api/3/users", **AUTH))
        assert [i["accountId"] for i in data] == ["a", "b"]
        assert [p["startAt"] for p in sent_params(m)] == ["0", "2"]


@pytest.mark.parametrize(
    "payload, match",
    [
        ({"errorMessages": ["Removed."]}, "'values'.*errorMessages"),
        ([{"id": "w1"}], "'values'.*list"),
    ],
    ids=["error_envelope", "bare_array"],
)
def test_missing_data_path_raises(payload: Any, match: str) -> None:
    with requests_mock.Mocker() as m:
        serve(m, "rest/api/3/workflow/search", payload)
        with pytest.raises(
            JiraResponseError, match="rest/api/3/workflow/search.*" + match
        ):
            list(
                get_paginated_data(
                    api_path="rest/api/3/workflow/search", data_path="values", **AUTH
                )
            )


def test_cursor_pagination_requires_object_response() -> None:
    with requests_mock.Mocker() as m:
        serve(m, "rest/api/3/search/jql", [{"id": "1"}])
        with pytest.raises(JiraResponseError, match="rest/api/3/search/jql.*list"):
            list(
                get_paginated_data(
                    api_path="rest/api/3/search/jql",
                    use_cursor_pagination=True,
                    **AUTH,
                )
            )


def test_http_error_raises() -> None:
    with requests_mock.Mocker() as m:
        m.get(url_for("rest/api/3/project/search"), status_code=403, json={})
        with pytest.raises(Exception, match="403"):
            list(
                get_paginated_data(
                    api_path="rest/api/3/project/search", data_path="values", **AUTH
                )
            )


def test_requests_carry_basic_auth() -> None:
    with requests_mock.Mocker() as m:
        serve(m, "rest/api/3/users", [])
        list(get_paginated_data(api_path="rest/api/3/users", **AUTH))
        header = m.request_history[0].headers["Authorization"]
        assert header.startswith("Basic ")
        assert b64decode(header.split(" ", 1)[1]).decode() == "e@example.com:token"


def test_workflows_requests_supported_path() -> None:
    # rest/api/3/workflow/search is deprecated and scheduled for removal
    api_path = DEFAULT_ENDPOINTS["workflows"]["api_path"]
    assert api_path == "rest/api/3/workflows/search"
    with requests_mock.Mocker() as m:
        serve(m, api_path, {"values": []})
        list(get_paginated_data(api_path=api_path, data_path="values", **AUTH))
        assert m.request_history[0].url.startswith(url_for(api_path))


def test_workflow_statuses_read_the_same_response_as_workflows() -> None:
    workflows = DEFAULT_ENDPOINTS["workflows"]
    statuses = DEFAULT_ENDPOINTS["workflow_statuses"]
    assert statuses["api_path"] == workflows["api_path"]
    # workflows__statuses.status_reference joins to this key
    assert statuses["primary_key"] == "status_reference"


@pytest.mark.parametrize(
    "endpoint_name", [n for n, e in DEFAULT_ENDPOINTS.items() if "deduplicate_on" in e]
)
def test_deduplicated_field_normalizes_to_the_primary_key(endpoint_name: str) -> None:
    """Deduplication runs on the response field, the primary key names the loaded column."""
    endpoint = DEFAULT_ENDPOINTS[endpoint_name]
    normalized = NamingConvention().normalize_identifier(endpoint["deduplicate_on"])
    assert normalized == endpoint["primary_key"]


def test_an_issue_matched_by_two_queries_is_yielded_once() -> None:
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/search/jql",
            {"issues": [{"id": "1"}, {"id": "2"}]},
            {"issues": [{"id": "2"}, {"id": "3"}]},
        )
        resource = jira_search(**AUTH).issues(
            jql_queries=["project = X", "project = Y"]
        )
        data = list(resource)
    assert [i["id"] for i in data] == ["1", "2", "3"]


def test_offset_path_advances_on_the_sibling_key() -> None:
    # workflow_statuses takes `statuses` but startAt counts `values`
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/workflows/search",
            {
                "values": [{"id": "w1"}, {"id": "w2"}],
                "statuses": [{"statusReference": "s1"}],
                "total": 3,
            },
            {
                "values": [{"id": "w3"}],
                "statuses": [{"statusReference": "s2"}],
                "total": 3,
            },
        )
        data = items(
            get_paginated_data(
                api_path="rest/api/3/workflows/search",
                data_path="statuses",
                offset_path="values",
                **AUTH,
            )
        )
        assert [i["statusReference"] for i in data] == ["s1", "s2"]
        # stepping by the rows taken would have sent startAt=1 and repeated w2
        assert [p["startAt"] for p in sent_params(m)] == ["0", "2"]


def test_offset_path_keeps_paging_past_a_page_with_no_rows_taken() -> None:
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/workflows/search",
            {"values": [{"id": "w1"}], "statuses": [], "total": 2},
            {
                "values": [{"id": "w2"}],
                "statuses": [{"statusReference": "s1"}],
                "total": 2,
            },
        )
        data = items(
            get_paginated_data(
                api_path="rest/api/3/workflows/search",
                data_path="statuses",
                offset_path="values",
                **AUTH,
            )
        )
        assert [i["statusReference"] for i in data] == ["s1"]
        assert m.call_count == 2


def assert_params_accepted(params: Dict[str, Any]) -> None:
    assert not (UNACCEPTED_PARAMS | INTERNAL_KEYS) & set(params)
    assert not UNACCEPTED_EXPANDS & set(params.get("expand", "").split(","))
    assert params["maxResults"] == str(AUTH["page_size"])


def assert_nullable_key(resource: Any, primary_key: str) -> None:
    key_column = resource.compute_table_schema()["columns"][primary_key]
    assert key_column["primary_key"] is True
    assert key_column["nullable"] is True


@pytest.mark.parametrize("endpoint_name", list(DEFAULT_ENDPOINTS))
def test_jira_resource_request(endpoint_name: str) -> None:
    endpoint = DEFAULT_ENDPOINTS[endpoint_name]
    empty_page: Any = (
        [] if endpoint_name == "users" else {"issues": [], "values": [], "statuses": []}
    )
    with requests_mock.Mocker() as m:
        serve(m, endpoint["api_path"], empty_page)
        resource = jira(**AUTH).resources[endpoint_name]
        assert list(resource) == []
        assert_params_accepted(sent_params(m)[0])

    assert_nullable_key(resource, endpoint["primary_key"])


def test_jira_search_request() -> None:
    with requests_mock.Mocker() as m:
        serve(m, "rest/api/3/search/jql", {"issues": []})
        resource = jira_search(**AUTH).issues(
            jql_queries=["project = X", "project = Y"]
        )
        assert list(resource) == []
        params = sent_params(m)
        assert [p["jql"] for p in params] == ["project = X", "project = Y"]
        for sent in params:
            assert_params_accepted(sent)
    assert_nullable_key(resource, "id")


def test_source_nesting_setting_is_not_overridden_on_search_issues() -> None:
    source = jira_search(**AUTH)
    source.max_table_nesting = 0
    assert source.max_table_nesting == 0
    # a resource level hint would win over the source setting, so there must not be one
    assert source.resources["issues"].max_table_nesting is None


def test_null_primary_key_loads_instead_of_failing_the_package() -> None:
    """The reason the primary key hint carries `nullable: True`."""
    pipeline = dlt.pipeline(
        pipeline_name="test_jira_nullable_key",
        destination="duckdb",
        dataset_name="test_jira_nullable_key",
        dev_mode=True,
    )
    with requests_mock.Mocker() as m:
        serve(
            m,
            DEFAULT_ENDPOINTS["users"]["api_path"],
            [{"accountId": "a", "displayName": "Someone"}, {"displayName": "Former"}],
            [],
        )
        info = pipeline.run(jira(**AUTH).with_resources("users"))
    assert_load_info(info)
    assert load_table_counts(pipeline, "users") == {"users": 2}


@pytest.mark.parametrize(
    "api_path, page, read_args",
    [
        (
            "rest/api/3/workflows/search",
            {"values": [{"id": "w1"}], "statuses": [], "isLast": True},
            {"data_path": "statuses", "offset_path": "values"},
        ),
        (
            "rest/api/3/search/jql",
            {"issues": [{"id": "1"}]},
            {"data_path": "issues", "use_cursor_pagination": True},
        ),
    ],
    ids=["offset", "cursor"],
)
def test_each_response_is_parsed_once_by_the_source(
    api_path: str,
    page: Any,
    read_args: Dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue pages run to megabytes and `Response.json()` parses the payload again."""
    parses = []
    original = Response.json

    def counting_json(self: Response, **kwargs: Any) -> Any:
        parses.append(self)
        return original(self, **kwargs)

    monkeypatch.setattr(Response, "json", counting_json)
    with requests_mock.Mocker() as m:
        serve(m, api_path, page)
        list(get_paginated_data(api_path=api_path, **read_args, **AUTH))
    # dlt parses the page to apply the data selector, this source parses it once more
    assert len(parses) == 2


def test_repeated_statuses_are_yielded_once() -> None:
    # workflows that share a status repeat it, and `replace` deduplicates nothing
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/workflows/search",
            {
                "values": [{"id": "w1"}],
                "statuses": [{"statusReference": "s1"}, {"statusReference": "s2"}],
                "total": 2,
            },
            {
                "values": [{"id": "w2"}],
                "statuses": [{"statusReference": "s2"}, {"statusReference": "s3"}],
                "total": 2,
            },
        )
        data = list(jira(**AUTH).resources["workflow_statuses"])
    assert [i["statusReference"] for i in data] == ["s1", "s2", "s3"]


def test_statuses_without_a_key_all_load() -> None:
    """A merge disposition collapses the keyless rows into one, `replace` lands them all."""
    pipeline = dlt.pipeline(
        pipeline_name="test_jira_keyless_statuses",
        destination="duckdb",
        dataset_name="test_jira_keyless_statuses",
        dev_mode=True,
    )
    # the keyless rows only disappear when the same load carries keyed ones
    statuses = [
        {"statusReference": "s1", "name": "Open"},
        {"name": "Unreferenced"},
        {"statusReference": "s2", "name": "Done"},
        {"name": "Also unreferenced"},
    ]
    with requests_mock.Mocker() as m:
        serve(
            m,
            DEFAULT_ENDPOINTS["workflow_statuses"]["api_path"],
            {"values": [{"id": "w1"}], "statuses": statuses, "isLast": True},
        )
        info = pipeline.run(jira(**AUTH).with_resources("workflow_statuses"))
    assert_load_info(info)
    assert load_table_counts(pipeline, "workflow_statuses") == {"workflow_statuses": 4}


def test_statuses_dropped_upstream_do_not_survive_the_next_load() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_jira_statuses_replace",
        destination="duckdb",
        dataset_name="test_jira_statuses_replace",
        dev_mode=True,
    )
    api_path = DEFAULT_ENDPOINTS["workflow_statuses"]["api_path"]
    loads = (
        [{"statusReference": "s1"}, {"statusReference": "s2"}],
        [{"statusReference": "s1"}],
    )
    for statuses in loads:
        with requests_mock.Mocker() as m:
            serve(
                m,
                api_path,
                {"values": [{"id": "w1"}], "statuses": statuses, "isLast": True},
            )
            info = pipeline.run(jira(**AUTH).with_resources("workflow_statuses"))
        assert_load_info(info)
    assert load_table_counts(pipeline, "workflow_statuses") == {"workflow_statuses": 1}
