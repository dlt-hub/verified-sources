import os
from datetime import date, datetime, timezone
from functools import partial
from pathlib import Path
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
    worklogs,
)
from sources.jira.settings import DEFAULT_ENDPOINTS, DEFAULT_LAG

from tests.jira.utils import (
    ALL_FIELD_IDS,
    BASE_URL,
    CREDENTIALS,
    CUSTOM,
    FIELDS,
    INTERNAL_KEYS,
    JiraMock,
    START,
    T1,
    UNACCEPTED_EXPANDS,
    UNACCEPTED_PARAMS,
    UPDATED_1,
    UPDATED_2,
    assert_nullable_key,
    duckdb_pipeline,
    filesystem_pipeline,
    load,
    make_comment,
    make_history,
    make_issue,
    make_worklog,
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


def test_issues_incremental_argument(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_2))
    # without incremental the JQL runs as it is on each run and no state is kept
    jql = "project = KAN ORDER BY created DESC"
    jira_mock.jql_results = {jql: ["2", "1"]}
    pipeline = duckdb_pipeline("jira_issues_no_incremental")
    for _ in range(2):
        load(pipeline, jira_issues(**CREDENTIALS, jql=jql, incremental=None))
    assert jira_mock.jqls == [jql, jql]
    assert load_table_counts(pipeline, "issues") == {"issues": 2}
    assert "sources" not in pipeline.state
    list(jira(**CREDENTIALS, incremental=None).with_resources("issues"))
    assert jira_mock.jqls[-1] == 'created >= "1970-01-01"'
    with pytest.raises(ValueError):
        jira_issues(**CREDENTIALS, jql=jql)
    with pytest.raises(ValueError):
        jira_issues(**CREDENTIALS, incremental=None, start_date=START)

    # an incremental without cursor, passed or in config, sets values of the default one
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 2, 1, tzinfo=timezone.utc)
    passed = jira_issues(
        **CREDENTIALS,
        incremental=dlt.sources.incremental(initial_value=start, end_value=end),
    )
    os.environ["SOURCES__JIRA__ISSUES__INCREMENTAL__INITIAL_VALUE"] = start.isoformat()
    os.environ["SOURCES__JIRA__ISSUES__INCREMENTAL__END_VALUE"] = end.isoformat()
    configured = jira_issues(**CREDENTIALS)
    for issues in (passed, configured):
        assert len(list(issues)) == 2
        assert jira_mock.jqls[-1] == (
            'updated >= "2024-01-01 00:00" AND updated < "2024-02-01 00:01"'
            " ORDER BY updated ASC"
        )
        incremental = issues._parent.incremental.incremental
        assert (incremental.cursor_path, incremental.lag) == ("updated", DEFAULT_LAG)
        assert issues.columns["updated"]["dedup_sort"] == "desc"

    # `start_date` wins over the initial value of an incremental without cursor
    since = datetime(2023, 6, 1, tzinfo=timezone.utc)
    partial = dlt.sources.incremental(initial_value=start, end_value=end)
    for issues in (
        jira_issues(**CREDENTIALS, start_date=since, incremental=partial),
        jira_issues(**CREDENTIALS, start_date=since),
    ):
        list(issues)
        assert jira_mock.jqls[-1] == (
            'updated >= "2023-06-01 00:00" AND updated < "2024-02-01 00:01"'
            " ORDER BY updated ASC"
        )


def test_jira_columns(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    list(jira(**CREDENTIALS, included_columns=["summary"]).with_resources("issues"))
    assert jira_mock.bulk_fetches[-1]["fields"] == ["summary", "updated"]
    list(jira(**CREDENTIALS, excluded_columns=()).with_resources("issues"))
    assert jira_mock.bulk_fetches[-1]["fields"] == ALL_FIELD_IDS

    # the `[sources.jira]` section reaches the source and the standalone resource
    os.environ["SOURCES__JIRA__EXCLUDED_COLUMNS"] = "[]"
    list(jira(**CREDENTIALS).with_resources("issues"))
    assert jira_mock.bulk_fetches[-1]["fields"] == ALL_FIELD_IDS
    list(jira_issues(**CREDENTIALS))
    assert jira_mock.bulk_fetches[-1]["fields"] == ALL_FIELD_IDS

    # the section of the resource holds the arguments `jira` does not take
    os.environ["SOURCES__JIRA__ISSUES__LAG"] = "60"
    source = jira(**CREDENTIALS).with_resources("issues")
    list(source)
    assert source.resources["issues"]._parent.incremental.incremental.lag == 60


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


def test_jira_replace(jira_mock: JiraMock, tmp_path: Path) -> None:
    # filesystem does not merge, resources that kept merge would be appended again
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_2))
    jira_mock.comments = {"1": [make_comment(10, UPDATED_1)]}
    jira_mock.histories = {"2": [make_history(200, UPDATED_2)]}
    jira_mock.worklogs = {"1": make_worklog(1, 1, T1)}
    jira_mock.deleted_worklogs = [{"worklogId": 2, "updatedTime": T1}]
    os.environ["SOURCES__JIRA__WRITE_DISPOSITION"] = "replace"
    pipeline = filesystem_pipeline("jira_replace", tmp_path.as_uri())
    tables = ("issues", "issue_comments", "issue_changelogs", "worklogs")
    for _ in range(2):
        source = jira(**CREDENTIALS).with_resources(*tables)
        load(pipeline, source, loader_file_format="parquet")
        assert load_table_counts(pipeline, *tables) == {
            "issues": 2,
            "issue_comments": 1,
            "issue_changelogs": 1,
            "worklogs": 1,
        }

    # each run reads all issues and worklogs, the incremental state is reset with the table
    assert jira_mock.jqls == ['created >= "1970-01-01" ORDER BY updated ASC'] * 2
    assert jira_mock.worklog_feeds == [{"feed": "updated", "since": 0}] * 2
    schema_tables = pipeline.default_schema.tables
    assert {schema_tables[table]["write_disposition"] for table in tables} == {
        "replace"
    }
    # the transformers of the module keep their write disposition
    assert issue_comments.write_disposition == "merge"


@pytest.mark.parametrize(
    "transformer", [issue_comments, issue_changelogs], ids=["comments", "changelogs"]
)
def test_transformers_not_from_issues(transformer: DltResource) -> None:
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(dlt.resource([{"id": "1"}], name="other") | transformer)
    assert isinstance(py_ex.value.__cause__, ValueError)


def with_query(query: str, row: Dict[str, Any]) -> Dict[str, Any]:
    return {**row, "query": query}


def test_issues_of_several_queries(search_mock: JiraMock) -> None:
    # each query is a resource with its own table, limit and config section
    os.environ["SOURCES__JIRA__EPICS__JQL"] = "project = Y"
    pipeline = duckdb_pipeline("jira_issues_queries")
    resources = [
        jira_issues(**CREDENTIALS, jql="project = X", name="bugs", chunk_size=1),
        jira_issues(**CREDENTIALS, name="epics", chunk_size=1),
    ]
    load(pipeline, [resource.add_limit(1) for resource in resources])
    assert load_table_counts(pipeline, "bugs", "epics") == {"bugs": 1, "epics": 1}
    assert sorted(search_mock.jqls) == [
        "(project = X) ORDER BY updated ASC",
        "(project = Y) ORDER BY updated ASC",
    ]
    assert {search["maxResults"] for search in search_mock.searches} == {"1"}
    assert len(search_mock.bulk_fetches) == 2

    # one table: a query column added by `add_map` is part of the primary key
    pipeline = duckdb_pipeline("jira_issues_one_table")
    resources = [
        jira_issues(**CREDENTIALS, jql=f"project = {project}", name=name)
        .add_map(partial(with_query, name))
        .apply_hints(table_name="issues", primary_key=["id", "query"])
        for name, project in (("bugs", "X"), ("epics", "Y"))
    ]
    load(pipeline, resources)
    rows = load_tables_to_dicts(pipeline, "issues")["issues"]
    assert sorted((row["id"], row["query"]) for row in rows) == [
        ("1", "bugs"),
        ("2", "bugs"),
        ("2", "epics"),
        ("3", "bugs"),
        ("4", "epics"),
    ]


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
