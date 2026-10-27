import os
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from typing import Any

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
)
from sources.jira.settings import DEFAULT_ENDPOINTS

from tests.jira.utils import (
    BASE_URL,
    CREDENTIALS,
    INTERNAL_KEYS,
    JiraMock,
    UNACCEPTED_EXPANDS,
    UNACCEPTED_PARAMS,
    UPDATED_1,
    UPDATED_2,
    assert_nullable_key,
    make_history,
    make_issue,
    sent_params,
    serve,
    to_rows,
)
from tests.utils import assert_load_info, load_table_counts, load_tables_to_dicts


def test_jira_source(http_mock: requests_mock.Mocker) -> None:
    # creating the source does not call the API: no routes are mocked
    source = jira(**CREDENTIALS)
    assert list(source.resources) == [
        "issues",
        "issue_comments",
        "issue_changelogs",
        "worklogs",
        "users",
        "workflows",
        "workflow_statuses",
        "projects",
    ]
    # the source setting reaches all resources, nesting of issues is bounded by field shapes
    assert all(
        resource.max_table_nesting is None for resource in source.resources.values()
    )
    write_dispositions = {
        name: resource.write_disposition for name, resource in source.resources.items()
    }
    assert write_dispositions == {
        "issues": "merge",
        "issue_comments": "merge",
        "issue_changelogs": "merge",
        "worklogs": "merge",
        "users": "replace",
        "workflows": "replace",
        "workflow_statuses": "replace",
        "projects": "replace",
    }

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
    rows = to_rows(list(source.with_resources("workflows")))
    assert [row["name"] for row in rows] == ["Flow"]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_issue_changelogs(jira_mock: JiraMock, backend: str) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in (1, 2, 3)))
    jira_mock.histories = {
        "1": [make_history(history_id, UPDATED_1) for history_id in (100, 101, 102)],
        "3": [
            make_history(300, UPDATED_2, field="assignee"),
            make_history(301, UPDATED_2),
        ],
    }
    jira_mock.changelog_page_size = 2
    issues = jira_issues(**CREDENTIALS, backend=backend)
    pipeline = dlt.pipeline(
        f"jira_changelogs_{backend}", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run([issues, issues | issue_changelogs]))

    assert [fetch.get("nextPageToken") for fetch in jira_mock.changelog_fetches] == [
        None,
        "2",
        "4",
    ]
    assert jira_mock.changelog_fetches[0]["issueIdsOrKeys"] == ["1", "2", "3"]
    assert load_table_counts(
        pipeline, "issue_changelogs", "issue_changelogs__items"
    ) == {
        "issue_changelogs": 5,
        "issue_changelogs__items": 5,
    }
    table = pipeline.default_schema.get_table("issue_changelogs")
    assert table["write_disposition"] == "merge"
    assert table["columns"]["created"]["data_type"] == "timestamp"
    rows = sorted(
        load_tables_to_dicts(pipeline, "issue_changelogs")["issue_changelogs"],
        key=lambda r: r["id"],
    )
    assert [(row["id"], row["issue_id"]) for row in rows] == [
        ("100", "1"),
        ("101", "1"),
        ("102", "1"),
        ("300", "3"),
        ("301", "3"),
    ]
    assert rows[3]["author__account_id"] == "a1"
    assert rows[3]["created"] == datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "transformer", [issue_comments, issue_changelogs], ids=["comments", "changelogs"]
)
def test_transformers_not_from_issues(transformer: DltResource) -> None:
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(dlt.resource([{"id": "1"}], name="other") | transformer)
    assert isinstance(py_ex.value.__cause__, ValueError)


# runs in a fresh interpreter in which pyarrow cannot be imported
NO_PYARROW_SCRIPT = textwrap.dedent(
    """
    import sys

    sys.modules["pyarrow"] = None

    import dlt
    import requests_mock
    from dlt.common.exceptions import MissingDependencyException

    from sources.jira import issue_changelogs, issue_comments, jira_issues, worklogs
    from tests.jira.utils import (
        CREDENTIALS,
        JiraMock,
        make_comment,
        make_history,
        make_issue,
        make_worklog,
    )

    work_dir = sys.argv[1]
    with requests_mock.Mocker() as m:
        jira_mock = JiraMock(m)
        jira_mock.add_issues(make_issue(1, "2024-01-15T10:30:00.123+0100"))
        jira_mock.histories["1"] = [make_history(100, "2024-01-15T10:30:00.000+0100")]
        jira_mock.comments["1"] = [make_comment(10, "2024-01-15T10:30:00.000+0100")]
        jira_mock.worklogs = {"7": make_worklog(7, 1, 1704103200000)}
        pipeline = dlt.pipeline(
            "jira_no_pyarrow",
            pipelines_dir=work_dir,
            destination=dlt.destinations.duckdb(f"{work_dir}/jira.duckdb"),
        )
        # nested types are ignored by the python backend
        issues = jira_issues(**CREDENTIALS, nested_types=True)
        info = pipeline.run(
            [
                issues,
                issues | issue_comments,
                issues | issue_changelogs,
                worklogs(**CREDENTIALS),
            ]
        )
        assert not info.has_failed_jobs
        print(sorted(pipeline.last_trace.last_normalize_info.row_counts.items()))
        try:
            jira_issues(**CREDENTIALS, backend="pyarrow")
        except MissingDependencyException:
            print("pyarrow backend requires pyarrow")
"""
)


def test_python_backend_without_pyarrow(tmp_path: Any) -> None:
    root_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    result = subprocess.run(
        [sys.executable, "-c", NO_PYARROW_SCRIPT, str(tmp_path)],
        cwd=root_dir,
        env={**os.environ, "PYTHONPATH": root_dir},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "[('_dlt_pipeline_state', 1), ('issue_changelogs', 1),"
        " ('issue_changelogs__items', 1), ('issue_comments', 1), ('issues', 1),"
        " ('issues__customfield_10031', 1), ('issues__fix_versions', 1), ('issues__labels', 2),"
        " ('worklogs', 1)]",
        "pyarrow backend requires pyarrow",
    ]


def test_jira_search(search_mock: JiraMock) -> None:
    source = jira_search(
        **CREDENTIALS, jql_queries=["project = X", "project = Y order by created"]
    )
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
    assert [search["jql"] for search in search_mock.searches] == [
        "project = X",
        "project = Y order by created",
    ]
    assert {search["fields"] for search in search_mock.searches} == {"id"}
    for sent in search_mock.searches:
        assert not (UNACCEPTED_PARAMS | INTERNAL_KEYS) & set(sent)
    assert len(search_mock.bulk_fetches) == 2


def test_jira_search_page_limit(search_mock: JiraMock) -> None:
    """A shared limit lets the first query absorb the budget and the second never runs."""
    source = jira_search(
        **CREDENTIALS,
        jql_queries=["project = X", "project = Y order by created"],
        page_size=1,
        page_limit=1,
    )
    pipeline = dlt.pipeline(
        "test_jira_search_page_limit", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(source))
    rows = load_tables_to_dicts(pipeline, "issues")["issues"]
    assert sorted((row["id"], row["query_index"]) for row in rows) == [
        ("1", 0),
        ("2", 1),
    ]
    table = pipeline.default_schema.get_table("issues")
    assert table["write_disposition"] == "replace"
    keys = [name for name, col in table["columns"].items() if col.get("primary_key")]
    assert keys == ["id", "query_index"]


@pytest.mark.parametrize("endpoint_name", list(DEFAULT_ENDPOINTS))
def test_endpoint_requests(endpoint_name: str) -> None:
    endpoint = DEFAULT_ENDPOINTS[endpoint_name]
    empty_page: Any = [] if endpoint_name == "users" else {"values": [], "statuses": []}
    # Jira caps `maxResults` per endpoint, so the default is per endpoint too
    for page_size, expected in ((None, endpoint["page_size"]), (7, 7)):
        with requests_mock.Mocker() as m:
            serve(m, endpoint["api_path"], empty_page)
            resource = jira(**CREDENTIALS, page_size=page_size).resources[endpoint_name]
            assert list(resource) == []
            (sent,) = sent_params(m)
        assert sent["maxResults"] == str(expected)
        assert not (UNACCEPTED_PARAMS | INTERNAL_KEYS) & set(sent)
        assert not UNACCEPTED_EXPANDS & set(sent.get("expand", "").split(","))
    assert_nullable_key(resource, endpoint["primary_key"])


def test_endpoint_resources_load() -> None:
    pipeline = dlt.pipeline("test_jira_endpoints", destination="duckdb", dev_mode=True)
    statuses_path = DEFAULT_ENDPOINTS["workflow_statuses"]["api_path"]
    loads = (
        # statuses without a key land with `replace`, merge would collapse them into one
        [
            {"statusReference": "s1"},
            {"statusReference": "s2"},
            {"name": "a"},
            {"name": "b"},
        ],
        [{"statusReference": "s1"}],
    )
    for statuses in loads:
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
                statuses_path,
                {"values": [{"id": "w1"}], "statuses": statuses, "isLast": True},
            )
            source = jira(**CREDENTIALS).with_resources("users", "workflow_statuses")
            assert_load_info(pipeline.run(source))
        if statuses is loads[0]:
            # a user without the key loads instead of failing the load package
            assert load_table_counts(pipeline, "users", "workflow_statuses") == {
                "users": 2,
                "workflow_statuses": 4,
            }
    # statuses dropped upstream do not survive the next load
    assert load_table_counts(pipeline, "workflow_statuses") == {"workflow_statuses": 1}
