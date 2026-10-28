import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import dlt
import pytest
import requests_mock
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources import DltResource

from sources.jira import (
    issue_changelogs,
    issue_comments,
    jira,
    jira_issues,
    jira_search,
    worklogs,
)
from sources.jira.settings import DEFAULT_ENDPOINTS

from tests.jira.utils import (
    BASE_URL,
    CREDENTIALS,
    CUSTOM,
    FIELDS,
    INTERNAL_KEYS,
    JiraMock,
    START,
    UNACCEPTED_EXPANDS,
    UNACCEPTED_PARAMS,
    UPDATED_1,
    assert_nullable_key,
    duckdb_pipeline,
    load,
    make_issue,
    rows_by_id,
    sent_params,
    serve,
)
from tests.utils import load_table_counts, load_tables_to_dicts


def test_jira_source(http_mock: requests_mock.Mocker) -> None:
    # creating the source does not call the API: no routes are mocked
    source = jira(**CREDENTIALS)
    assert list(source.resources) == [
        "issues",
        "issue_comments",
        "issue_changelogs",
        "worklogs",
        "issue_fields",
        "users",
        "workflows",
        "workflow_statuses",
        "projects",
    ]
    http_mock.get(
        BASE_URL + "rest/api/3/workflows/search",
        [
            {
                "json": {
                    "values": [{"id": {"entityId": "w1"}, "name": "Flow"}],
                    "isLast": True,
                }
            },
            {"json": {"values": [], "isLast": True}},
        ],
    )
    rows = list(source.with_resources("workflows"))
    assert [row["name"] for row in rows] == ["Flow"]


@pytest.mark.parametrize(
    "kwargs,expected_jql,expected_since",
    [
        (
            {"start_date": date(2024, 1, 15)},
            'updated >= "2024-01-15 00:00" ORDER BY updated ASC',
            1705276800000,
        ),
        ({}, 'created >= "1970-01-01" ORDER BY updated ASC', 0),
        (
            {"incremental": dlt.sources.incremental("created", initial_value=START)},
            'created >= "2024-01-15 09:30" ORDER BY created ASC',
            0,
        ),
    ],
    ids=["start_date", "default", "own_incremental"],
)
def test_jira_source_incremental(
    jira_mock: JiraMock, kwargs: Dict[str, Any], expected_jql: str, expected_since: int
) -> None:
    # a start date starts issues and worklogs, an incremental applies to issues only
    source = jira(**CREDENTIALS, **kwargs)
    assert list(source.with_resources("issues", "worklogs")) == []
    assert jira_mock.jqls == [expected_jql]
    assert jira_mock.worklog_feeds[0] == {"feed": "updated", "since": expected_since}


def test_issues_config_resolution(jira_mock: JiraMock) -> None:
    os.environ["SOURCES__JIRA__SUBDOMAIN"] = CREDENTIALS["subdomain"]
    os.environ["SOURCES__JIRA__EMAIL"] = CREDENTIALS["email"]
    os.environ["SOURCES__JIRA__API_TOKEN"] = CREDENTIALS["api_token"]
    os.environ["SOURCES__JIRA__ISSUES__LAG"] = "60"
    jira_mock.add_issues(make_issue(1, UPDATED_1))

    issues = jira_issues()
    assert (issues.name, issues._parent.name) == ("issues", "issues_ids")
    assert (issues.section, issues._parent.section) == ("jira", "jira")
    assert issues.write_disposition == "merge"
    assert [row["id"] for row in issues] == ["1"]
    # the incremental is bound when the ids resource runs
    incremental = issues._parent.incremental.incremental
    assert (incremental.cursor_path, incremental.initial_value, incremental.lag) == (
        "updated",
        None,
        60,
    )
    assert issues.columns["updated"]["dedup_sort"] == "desc"

    # a start date is the initial value of the default incremental
    os.environ["SOURCES__JIRA__ISSUES__START_DATE"] = "2024-01-15"
    started = jira_issues()
    assert len(list(started)) == 1
    incremental = started._parent.incremental.incremental
    assert (incremental.cursor_path, incremental.initial_value, incremental.lag) == (
        "updated",
        datetime(2024, 1, 15, tzinfo=timezone.utc),
        60,
    )
    assert started.columns["updated"]["dedup_sort"] == "desc"
    with pytest.raises(ValueError):
        jira_issues(incremental=dlt.sources.incremental("created"))
    del os.environ["SOURCES__JIRA__ISSUES__START_DATE"]

    # your own incremental is configurable like any dlt incremental
    os.environ["SOURCES__JIRA__ISSUES__INCREMENTAL__CURSOR_PATH"] = "created"
    created = jira_issues()
    assert created.write_disposition == "merge"
    assert len(list(created)) == 1
    assert created._parent.incremental.incremental.cursor_path == "created"
    assert jira_mock.jqls[-1] == 'created >= "1970-01-01" ORDER BY created ASC'
    assert "updated" not in created.columns
    del os.environ["SOURCES__JIRA__ISSUES__INCREMENTAL__CURSOR_PATH"]

    # resources of issues with their own names keep their own tables and state. resources run
    # together share the `jira` section, so the worklogs find the secrets too
    pipeline = duckdb_pipeline("jira_issues_named")
    load(
        pipeline,
        [
            jira_issues(name="bugs", jql="type = Bug"),
            jira_issues(name="tasks"),
            worklogs(),
        ],
    )
    assert load_table_counts(pipeline, "bugs", "tasks") == {"bugs": 1, "tasks": 1}
    assert jira_mock.jqls[-2:] == [
        "(type = Bug) ORDER BY updated ASC",
        'created >= "1970-01-01" ORDER BY updated ASC',
    ]
    resource_states = [
        set(source_state["resources"])
        for source_state in pipeline.state["sources"].values()
    ]
    assert any({"bugs_ids", "tasks_ids"} <= names for names in resource_states)


def test_issue_fields(jira_mock: JiraMock) -> None:
    pipeline = duckdb_pipeline("jira_issue_fields")
    load(pipeline, jira(**CREDENTIALS).with_resources("issue_fields"))
    assert jira_mock.searches == []
    table = pipeline.default_schema.get_table("issue_fields")
    assert table["write_disposition"] == "replace"
    assert table["columns"]["id"]["primary_key"] is True
    rows = {row["id"]: row for row in rows_by_id(pipeline, "issue_fields")}
    assert len(rows) == len(FIELDS)
    # custom field columns of `issues` are named by the field id
    assert rows["customfield_10016"]["name"] == "Custom 10016"
    assert rows["customfield_10016"]["schema__custom"] == CUSTOM + "float"
    assert rows["customfield_10016"]["custom"] is True


@pytest.mark.parametrize(
    "transformer", [issue_comments, issue_changelogs], ids=["comments", "changelogs"]
)
def test_transformers_not_from_issues(transformer: DltResource) -> None:
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(dlt.resource([{"id": "1"}], name="other") | transformer)
    assert isinstance(py_ex.value.__cause__, ValueError)


def test_jira_search(search_mock: JiraMock) -> None:
    queries = ["project = X", "project = Y order by created"]
    source = jira_search(**CREDENTIALS, jql_queries=queries)
    # a resource level hint would win over the source setting, so there must not be one
    assert source.resources["issues"].max_table_nesting is None
    rows = list(source)
    # the query index tells which query found the issue, pages are fetched in parallel
    assert sorted((row["id"], row["query_index"]) for row in rows) == [
        ("1", 0),
        ("2", 0),
        ("2", 1),
        ("3", 0),
        ("4", 1),
    ]
    # queries run as they are, search lists ids only and issues are fetched in bulk
    assert [search["jql"] for search in search_mock.searches] == queries
    assert {search["fields"] for search in search_mock.searches} == {"id"}
    for sent in search_mock.searches:
        assert not (UNACCEPTED_PARAMS | INTERNAL_KEYS) & set(sent)
    assert len(search_mock.bulk_fetches) == 2

    # the page limit bounds each query, a shared limit would let the first query absorb it
    source = jira_search(**CREDENTIALS, jql_queries=queries, page_size=1, page_limit=1)
    pipeline = duckdb_pipeline("jira_search")
    load(pipeline, source)
    rows = load_tables_to_dicts(pipeline, "issues")["issues"]
    assert sorted((row["id"], row["query_index"]) for row in rows) == [
        ("1", 0),
        ("2", 1),
    ]
    table = pipeline.default_schema.get_table("issues")
    assert table["write_disposition"] == "replace"
    keys = [name for name, col in table["columns"].items() if col.get("primary_key")]
    assert keys == ["id", "query_index"]


@pytest.mark.parametrize("page_size", [None, 7], ids=["default", "explicit"])
@pytest.mark.parametrize("endpoint_name", list(DEFAULT_ENDPOINTS))
def test_endpoint_requests(endpoint_name: str, page_size: Optional[int]) -> None:
    endpoint = DEFAULT_ENDPOINTS[endpoint_name]
    empty_page: Any = [] if endpoint_name == "users" else {"values": [], "statuses": []}
    with requests_mock.Mocker() as m:
        serve(m, endpoint["api_path"], empty_page)
        resource = jira(**CREDENTIALS, page_size=page_size).resources[endpoint_name]
        assert list(resource) == []
        (sent,) = sent_params(m)
    # Jira caps `maxResults` per endpoint, so the default is per endpoint too
    assert sent["maxResults"] == str(page_size or endpoint["page_size"])
    assert not (UNACCEPTED_PARAMS | INTERNAL_KEYS) & set(sent)
    assert not UNACCEPTED_EXPANDS & set(sent.get("expand", "").split(","))
    assert_nullable_key(resource, endpoint["primary_key"])


def test_endpoint_resources_load() -> None:
    pipeline = duckdb_pipeline("jira_endpoints")

    def run(statuses: List[Any]) -> None:
        with requests_mock.Mocker() as m:
            serve(
                m,
                DEFAULT_ENDPOINTS["users"]["api_path"],
                [
                    {"accountId": "a", "displayName": "Someone"},
                    {"displayName": "Former"},
                ],
                [],
            )
            serve(
                m,
                DEFAULT_ENDPOINTS["workflow_statuses"]["api_path"],
                {"values": [{"id": "w1"}], "statuses": statuses, "isLast": True},
            )
            load(
                pipeline,
                jira(**CREDENTIALS).with_resources("users", "workflow_statuses"),
            )

    # statuses without a key land with `replace`, merge would collapse them into one
    run(
        [
            {"statusReference": "s1"},
            {"statusReference": "s2"},
            {"name": "a"},
            {"name": "b"},
        ]
    )
    # a user without the key loads instead of failing the load package
    assert load_table_counts(pipeline, "users", "workflow_statuses") == {
        "users": 2,
        "workflow_statuses": 4,
    }
    # statuses dropped upstream do not survive the next load
    run([{"statusReference": "s1"}])
    assert load_table_counts(pipeline, "workflow_statuses") == {"workflow_statuses": 1}
