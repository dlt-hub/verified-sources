import copy
import json
import time
from datetime import date, datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

import dlt
import pyarrow as pa
import pytest
import requests_mock
from pytest_mock import MockerFixture

from sources.jira import issue_changelogs, issue_comments, jira, jira_issues
from sources.jira.helpers.client import JiraClient
from sources.jira.helpers.issues import changelog_rows
from sources.jira.helpers.schema_types import get_field_type, shape_to_arrow

from tests.jira.utils import (
    CREDENTIALS,
    DEFAULT_FIELD_IDS,
    JiraMock,
    UPDATED_1,
    UPDATED_2,
    USER,
    load_captured,
    make_comment,
    make_history,
    make_issue,
    run_to_parquet,
    to_rows,
)
from tests.utils import assert_load_info, load_table_counts, load_tables_to_dicts


def test_issues_python_backend(
    jira_mock: JiraMock, http_mock: requests_mock.Mocker
) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_2, labels=[]))
    pipeline = dlt.pipeline("jira_issues_python", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS)))

    assert load_table_counts(
        pipeline,
        "issues",
        "issues__labels",
        "issues__fix_versions",
        "issues__customfield_10031",
    ) == {
        "issues": 2,
        "issues__labels": 2,
        "issues__fix_versions": 2,
        "issues__customfield_10031": 2,
    }
    columns = pipeline.default_schema.get_table_columns("issues")
    assert {
        name: columns[name]["data_type"] for name in columns if not name.startswith("_")
    } == {
        "id": "text",
        "key": "text",
        "summary": "text",
        "description": "text",
        "created": "timestamp",
        "updated": "timestamp",
        "duedate": "date",
        "timespent": "bigint",
        "customfield_10016": "double",
        "customfield_10032": "date",
        "customfield_10033": "timestamp",
        # values of unknown structure are text
        "customfield_10050": "text",
        "thumbnail": "text",
        # objects are flattened and trimmed to known keys
        "status__id": "text",
        "status__name": "text",
        "status__status_category__id": "bigint",
        "status__status_category__key": "text",
        "status__status_category__name": "text",
        "project__id": "text",
        "project__key": "text",
        "project__name": "text",
        "assignee__account_id": "text",
        "assignee__display_name": "text",
        "assignee__active": "bool",
        "assignee__account_type": "text",
        "assignee__time_zone": "text",
        "customfield_10030__id": "text",
        "customfield_10030__value": "text",
    }
    rows = sorted(
        load_tables_to_dicts(pipeline, "issues")["issues"], key=lambda r: r["id"]
    )
    assert [row["updated"] for row in rows] == [
        datetime(2024, 1, 15, 9, 30, 0, 123000, tzinfo=timezone.utc),
        datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc),
    ]
    assert rows[0]["key"] == "KAN-1"
    assert rows[0]["description"] == "h1. Title\n*bold*"
    assert rows[0]["duedate"] == date(2024, 2, 1)
    assert rows[0]["status__status_category__key"] == "indeterminate"

    # captured issues load into flattened columns and nested tables
    captured = load_captured()
    jira_mock = JiraMock(http_mock, fields=captured["fields"])
    jira_mock.add_issues(*captured["issues"])
    pipeline = dlt.pipeline("jira_captured_python", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS)))

    issues = {issue["key"]: issue["fields"] for issue in captured["issues"]}
    assert load_table_counts(
        pipeline,
        "issues",
        "issues__subtasks",
        "issues__issuelinks",
        "issues__customfield_10020",
    ) == {
        "issues": 8,
        "issues__subtasks": 2,
        "issues__issuelinks": sum(
            len(f.get("issuelinks", [])) for f in issues.values()
        ),
        "issues__customfield_10020": 1,
    }
    rows = {
        row["key"]: row for row in load_tables_to_dicts(pipeline, "issues")["issues"]
    }
    assert rows["TST-19"]["parent__key"] == "TST-18"
    assert rows["TST-24"]["customfield_10046__child__value"] == "[dummy] rest_api"
    assert rows["TST-24"]["timetracking__original_estimate_seconds"] == 7200
    assert rows["TST-24"]["customfield_10019"] == "0|i0008n:"


def test_issues_pyarrow_backend(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_2))
    pipeline = dlt.pipeline("jira_issues_pyarrow", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS, backend="pyarrow")))

    # nested values are loaded as json, there are no nested tables
    assert [table["name"] for table in pipeline.default_schema.data_tables()] == [
        "issues"
    ]
    columns = pipeline.default_schema.get_table_columns("issues")
    assert {
        name: columns[name]["data_type"] for name in columns if not name.startswith("_")
    } == {
        "id": "text",
        "key": "text",
        "summary": "text",
        "description": "text",
        "created": "timestamp",
        "updated": "timestamp",
        "duedate": "date",
        "status": "json",
        "project": "json",
        "assignee": "json",
        "labels": "json",
        "fix_versions": "json",
        "timespent": "bigint",
        "attachment": "json",
        "customfield_10016": "double",
        "customfield_10030": "json",
        "customfield_10031": "json",
        "customfield_10032": "date",
        "customfield_10033": "timestamp",
        "customfield_10050": "text",
        "thumbnail": "text",
        "parent": "json",
    }
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT updated, status, labels, customfield_10050 FROM issues ORDER BY id"
        )
    assert rows[0][0] == datetime(2024, 1, 15, 9, 30, 0, 123000, tzinfo=timezone.utc)
    assert rows[1][0] == datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc)
    # trimmed to known keys, nested keys are not normalized
    assert rows[0][1] == (
        '{"id":"3","name":"In Progress","statusCategory":{"id":4,"key":"indeterminate",'
        '"name":"In Progress"}}'
    )
    assert rows[0][2] == '["backend","urgent"]'
    assert rows[0][3] == '{"anything":["goes",1]}'


def test_issues_nested_types(http_mock: requests_mock.Mocker) -> None:
    captured_mock = load_captured()
    jira_mock = JiraMock(http_mock, fields=captured_mock["fields"])
    jira_mock.add_issues(*captured_mock["issues"])
    # a string instead of an object and an object instead of a list
    unexpected = copy.deepcopy(captured_mock["issues"][0])
    unexpected["id"], unexpected["key"] = "99999", "TST-999"
    unexpected["fields"]["customfield_10046"] = "legacy"
    unexpected["fields"]["customfield_10045"] = {"id": "1"}
    jira_mock.add_issues(unexpected)
    pipeline, table = run_to_parquet(
        "jira_captured_nested",
        jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True),
    )

    # fields of known shape have nested types
    naming = pipeline.default_schema.naming
    shapes = {field["id"]: get_field_type(field) for field in captured_mock["fields"]}
    nested_shapes = {k: v for k, v in shapes.items() if isinstance(v, (dict, list))}
    assert sorted(nested_shapes) == [
        "aggregateprogress",
        "assignee",
        "attachment",
        "components",
        "creator",
        "customfield_10020",
        "customfield_10021",
        "customfield_10036",
        "customfield_10044",
        "customfield_10045",
        "customfield_10046",
        "customfield_10051",
        "customfield_10052",
        "fixVersions",
        "issuelinks",
        "issuetype",
        "labels",
        "parent",
        "priority",
        "progress",
        "project",
        "reporter",
        "status",
        "statusCategory",
        "subtasks",
        "timetracking",
        "versions",
        "votes",
        "watches",
    ]
    for field_id, shape in nested_shapes.items():
        column = table.schema.field(naming.normalize_identifier(field_id))
        assert column.type == shape_to_arrow(shape), field_id

    rows = {row["key"]: row for row in table.to_pylist()}
    assert len(rows) == 9
    # values of unexpected structure are loaded as null
    assert rows["TST-999"]["customfield_10046"] is None
    assert rows["TST-999"]["customfield_10045"] is None
    assert rows["TST-13"]["customfield_10020"] == [
        {
            "id": 4,
            "name": "TST Sprint 3",
            "state": "future",
            "boardId": 2,
            "goal": "[dummy] schema contracts",
            "startDate": None,
            "endDate": None,
            "completeDate": None,
        }
    ]
    all_types = rows["TST-24"]
    assert all_types["customfield_10046"] == {
        "id": "10027",
        "value": "[dummy] sources",
        "child": {"id": "10029", "value": "[dummy] rest_api"},
    }
    assert [option["value"] for option in all_types["customfield_10045"]] == [
        "[dummy] duckdb",
        "[dummy] bigquery",
    ]
    assert all_types["timetracking"] == {
        "originalEstimateSeconds": 7200,
        "remainingEstimateSeconds": 5400,
        "timeSpentSeconds": None,
    }
    (attachment,) = rows["TST-17"]["attachment"]
    assert attachment["filename"] == "dlt-seed.csv"
    assert attachment["mimeType"] == "text/csv"
    assert attachment["author"] == {
        "accountId": "account-1",
        "displayName": "User account-1",
    }
    assert [subtask["key"] for subtask in rows["TST-18"]["subtasks"]] == [
        "TST-19",
        "TST-20",
    ]
    assert rows["TST-19"]["parent"] == {"id": "10034", "key": "TST-18"}
    assert sorted(
        (link["type"]["name"], link["outwardIssue"]["key"], link["inwardIssue"])
        for link in rows["TST-18"]["issuelinks"]
    ) == [("Blocks", "TST-21", None), ("Relates", "TST-21", None)]
    # rank is text, values of unknown structure are serialized as json
    assert all_types["customfield_10019"] == "0|i0008n:"
    assert all_types["customfield_10002"] == "[]"
    # restrictions have dynamic keys and are not loaded by default
    assert "issuerestriction" not in table.column_names


def test_issues_captured_parallel_pages(
    captured_mock: Dict[str, Any], mocker: MockerFixture
) -> None:
    # requests_mock serializes requests, the delay is added around them to overlap the fetches
    bulk_fetch = JiraClient.bulk_fetch
    lock, fetches = Lock(), {"in_flight": 0, "max_parallel": 0}

    def slow_bulk_fetch(
        client: JiraClient, ids: List[str], fields: List[str], batch_size: int
    ) -> Any:
        with lock:
            fetches["in_flight"] += 1
            fetches["max_parallel"] = max(fetches["max_parallel"], fetches["in_flight"])
        time.sleep(0.1)
        try:
            return bulk_fetch(client, ids, fields, batch_size)
        finally:
            with lock:
                fetches["in_flight"] -= 1

    mocker.patch.object(JiraClient, "bulk_fetch", slow_bulk_fetch)
    _, single_page = run_to_parquet(
        "jira_single_page",
        jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True),
    )
    assert fetches["max_parallel"] == 1

    # a page per issue, pages are fetched in parallel by dlt extract workers
    _, parallel_pages = run_to_parquet(
        "jira_parallel_pages",
        jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True, chunk_size=1),
    )
    assert fetches["max_parallel"] > 1

    # column hints reach the extractor before any of the pages
    def comparable(table: pa.Table) -> pa.Table:
        return table.drop_columns(
            [name for name in table.column_names if name.startswith("_dlt")]
        ).sort_by("key")

    single_page, parallel_pages = comparable(single_page), comparable(parallel_pages)
    assert parallel_pages.schema == single_page.schema
    assert parallel_pages.to_pylist() == single_page.to_pylist()


def test_issues_pagination(jira_mock: JiraMock) -> None:
    # nothing is fetched for issues, comments and changelogs when search finds no issues
    issues = jira_issues(**CREDENTIALS)
    assert to_rows(list(issues | issue_changelogs)) == []
    assert to_rows(list(issues | issue_comments)) == []
    assert len(jira_mock.searches) == 2
    assert jira_mock.bulk_fetches == []
    assert jira_mock.changelog_fetches == []
    assert jira_mock.comment_fetches == []

    jira_mock.searches.clear()
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in range(1, 6)))
    jira_mock.search_pages = [["1", "2"], ["3", "4"], ["5"]]

    issues = jira_issues(
        **CREDENTIALS,
        backend="pyarrow",
        chunk_size=3,
        bulk_fetch_size=2,
        search_page_size=2,
    )
    pages = list(issues)

    # ids found by search are fetched in pages of `chunk_size`, pages are fetched in parallel
    assert sorted(page.num_rows for page in pages) == [2, 3]
    assert sorted(row["id"] for row in to_rows(pages)) == ["1", "2", "3", "4", "5"]
    assert [search.get("nextPageToken") for search in jira_mock.searches] == [
        None,
        "1",
        "2",
    ]
    assert len({search["jql"] for search in jira_mock.searches}) == 1
    assert {search["maxResults"] for search in jira_mock.searches} == {"2"}
    # each page is fetched in batches of `bulk_fetch_size`
    assert sorted(fetch["issueIdsOrKeys"] for fetch in jira_mock.bulk_fetches) == [
        ["1", "2"],
        ["3"],
        ["4", "5"],
    ]


def test_issues_bulk_fetch_batches(http_mock: requests_mock.Mocker) -> None:
    for included_columns, max_bulk_issues, expected_batches in (
        # an explicit list of max 100 single value fields
        (["summary"], None, [50, 1000]),
        # attachments and other multi value fields
        (None, None, [50] + [100] * 10),
        # batches of 100 after Jira rejected a larger one
        (["summary"], 100, [50] + [100] * 10 + [1000]),
    ):
        jira_mock = JiraMock(http_mock)
        jira_mock.add_issues(
            *(make_issue(issue_id, UPDATED_1) for issue_id in range(1, 1051))
        )
        jira_mock.max_bulk_issues = max_bulk_issues
        pages = list(
            jira_issues(
                **CREDENTIALS, backend="pyarrow", included_columns=included_columns
            )
        )
        # pages hold 1000 issues, fetched with the largest requests the fields allow
        assert sorted(page.num_rows for page in pages) == [50, 1000]
        assert (
            sorted(len(fetch["issueIdsOrKeys"]) for fetch in jira_mock.bulk_fetches)
            == expected_batches
        ), included_columns


def test_issues_limit_backfill(jira_mock: JiraMock) -> None:
    # issues updated an hour apart are read in pages of 2
    jira_mock.add_issues(
        *(make_issue(n, f"2024-01-15T1{n}:00:00.000+0000") for n in range(1, 6))
    )
    pipeline = dlt.pipeline("jira_issues_backfill", destination="duckdb", dev_mode=True)
    loaded = []
    for _ in range(3):
        issues = jira_issues(**CREDENTIALS, chunk_size=2, lag=0)
        # transformers process all pages they receive, the limit is set on the resource with ids
        issues._parent.add_limit(1)
        assert_load_info(pipeline.run(issues))
        rows = load_tables_to_dicts(pipeline, "issues")["issues"]
        loaded.append(sorted(row["id"] for row in rows))

    # each run loads the next page of issues and continues from its last update
    assert loaded == [["1", "2"], ["1", "2", "3", "4"], ["1", "2", "3", "4", "5"]]
    assert jira_mock.jqls == [
        'created >= "1970-01-01" ORDER BY updated ASC',
        'updated >= "2024-01-15 12:00" ORDER BY updated ASC',
        'updated >= "2024-01-15 14:00" ORDER BY updated ASC',
    ]


def test_issues_deleted_after_search(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(3, UPDATED_1))
    jira_mock.search_pages = [["1", "2", "3"]]

    # the deleted issue has no cursor value in the mocked search results
    rows = to_rows(list(jira_issues(**CREDENTIALS, incremental=None)))

    assert [row["id"] for row in rows] == ["1", "3"]


def test_issues_unexpected_values(jira_mock: JiraMock) -> None:
    issue_without_project = make_issue(1, UPDATED_1)
    del issue_without_project["fields"]["project"]
    jira_mock.add_issues(
        issue_without_project,
        make_issue(2, UPDATED_1, project=None),
        make_issue(
            3,
            UPDATED_1,
            customfield_10030="legacy",
            customfield_10016={"value": 1},
            customfield_10031=USER,
        ),
    )

    # the python backend skips empty fields
    rows = to_rows(list(jira_issues(**CREDENTIALS)))
    assert [row.get("project") for row in rows] == [None, None, rows[2]["project"]]
    assert rows[2]["project"] is not None
    # values of unexpected structure are loaded as null
    assert rows[2].get("customfield_10030") is None
    assert rows[2].get("customfield_10016") is None
    assert rows[0]["customfield_10030"] is not None

    # duckdb does not support nested types, nested values are json
    pipeline = dlt.pipeline(
        "jira_unexpected_values", destination="duckdb", dev_mode=True
    )
    issues = jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True)
    assert_load_info(pipeline.run(issues))
    with pipeline.sql_client() as client:
        types = dict(
            client.execute_sql(
                "SELECT column_name, data_type FROM information_schema.columns"
                " WHERE table_name = 'issues'"
            )
        )
        records = client.execute_sql(
            "SELECT id, customfield_10030, customfield_10031 FROM issues ORDER BY id"
        )
    assert types["customfield_10030"] == types["customfield_10031"] == "JSON"
    assert json.loads(records[0][1])["value"] == "Red"
    assert [user["accountId"] for user in json.loads(records[0][2])] == ["a1"]
    assert records[2][1:] == (None, None)


def test_issues_incremental_runs(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(
        make_issue(1, "2024-01-15T10:30:00.000+0000"),
        make_issue(2, "2024-01-16T08:00:00.000+0000"),
    )
    pipeline = dlt.pipeline(
        "jira_issues_incremental", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS)))
    table = pipeline.default_schema.get_table("issues")
    assert table["write_disposition"] == "merge"
    columns = pipeline.default_schema.get_table_columns("issues")
    assert columns["id"]["primary_key"] is True
    assert columns["updated"]["dedup_sort"] == "desc"

    # issue 2 was updated within lag and is read again, issue 1 was updated
    jira_mock.add_issues(
        make_issue(1, "2024-01-17T12:00:45.000+0000", summary="changed")
    )
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS)))

    assert jira_mock.jqls == [
        'created >= "1970-01-01" ORDER BY updated ASC',
        # the last value minus 10 minutes of lag
        'updated >= "2024-01-16 07:50" ORDER BY updated ASC',
    ]
    rows = sorted(
        load_tables_to_dicts(pipeline, "issues")["issues"], key=lambda r: r["id"]
    )
    assert [(row["id"], row["summary"]) for row in rows] == [
        ("1", "changed"),
        ("2", "Issue 2"),
    ]


@pytest.mark.parametrize(
    "included_columns,excluded_columns,expected_fields",
    [
        (["summary", "status", "colour"], None, ["summary", "status", "updated"]),
        (["summary", "updated", "comment"], None, ["summary", "updated", "comment"]),
        (
            None,
            ["description", "customfield_10050"],
            [
                field_id
                for field_id in DEFAULT_FIELD_IDS
                if field_id not in ("description", "customfield_10050")
            ],
        ),
    ],
    ids=["included", "included_comments", "excluded"],
)
def test_issues_columns(
    jira_mock: JiraMock,
    included_columns: Optional[List[str]],
    excluded_columns: Optional[List[str]],
    expected_fields: List[str],
) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    rows = to_rows(
        list(
            jira_issues(
                **CREDENTIALS,
                included_columns=included_columns,
                excluded_columns=excluded_columns,
                # keeps empty fields
                backend="pyarrow",
            )
        )
    )
    assert jira_mock.bulk_fetches[0]["fields"] == expected_fields
    assert list(rows[0]) == ["id", "key", *expected_fields]


def test_issues_user_column_hints(jira_mock: JiraMock, mocker: MockerFixture) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    issues = jira_issues(**CREDENTIALS)
    issues.apply_hints(columns={"customfield_10016": {"data_type": "text"}})
    pipeline = dlt.pipeline("jira_issues_hints", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(issues))
    columns = pipeline.default_schema.get_table_columns("issues")
    assert columns["customfield_10016"]["data_type"] == "text"
    assert columns["updated"]["data_type"] == "timestamp"

    # dynamic hints cannot set the types of Arrow tables
    warning = mocker.patch("sources.jira.helpers.issues.logger.warning")
    issues = jira_issues(**CREDENTIALS, backend="pyarrow")
    # dlt requires a dynamic table name with other dynamic hints
    issues.apply_hints(
        table_name=lambda item: "issues",
        columns=lambda item: {"summary": {"data_type": "text"}},
    )
    assert len(to_rows(list(issues))) == 1
    assert "cannot be applied with the pyarrow backend" in warning.call_args.args[0]


@pytest.mark.parametrize(
    "resources,expected_fields",
    [
        (["issues"], [DEFAULT_FIELD_IDS]),
        (["issue_comments"], [["comment"]]),
        (["issues", "issue_comments"], [[*DEFAULT_FIELD_IDS, "comment"]]),
        (["issue_changelogs"], []),
        (["issue_comments", "issue_changelogs"], [["comment"]]),
    ],
    ids=[
        "issues",
        "comments",
        "issues_and_comments",
        "changelogs",
        "comments_changelogs",
    ],
)
def test_issues_fetch_selected_resources(
    jira_mock: JiraMock, resources: List[str], expected_fields: List[List[str]]
) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in (1, 2)))
    jira_mock.comments = {"1": [make_comment(10, UPDATED_1)]}
    jira_mock.histories = {"2": [make_history(200, UPDATED_1)]}
    pipeline = dlt.pipeline(
        "jira_selected_resources", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(jira(**CREDENTIALS).with_resources(*resources)))

    # selected resources are loaded with a single request per batch of issues
    assert [fetch["fields"] for fetch in jira_mock.bulk_fetches] == expected_fields
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert {name for name in row_counts if "__" not in name} == {
        "_dlt_pipeline_state",
        *resources,
    }


def test_changelog_rows(jira_mock: JiraMock) -> None:
    jira_mock.histories = {
        "1": [make_history(history_id, UPDATED_1) for history_id in range(5)]
    }
    jira_mock.changelog_page_size = 2
    issues = [{"id": str(issue_id)} for issue_id in range(1001)]
    pages = list(changelog_rows(JiraClient(**CREDENTIALS), issues, chunk_size=3))
    # rows of API pages of 2 changes are yielded in pages of 3
    assert [len(page) for page in pages] == [3, 2]
    # changes are fetched for max 1000 issues, paged with a token
    assert [len(fetch["issueIdsOrKeys"]) for fetch in jira_mock.changelog_fetches] == [
        1000,
        1000,
        1000,
        1,
    ]
