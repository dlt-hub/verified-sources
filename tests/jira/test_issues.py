from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import dlt
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.pipeline.helpers import pipeline_drop
from pytest_mock import MockerFixture

from sources.jira import issue_changelogs, issue_comments, jira, jira_issues
from sources.jira.helpers.client import JiraClient
from sources.jira.helpers.issues import IssuesReader, changelog_rows, issue_ids

from tests.jira.utils import (
    CREDENTIALS,
    DEFAULT_FIELD_IDS,
    JiraMock,
    UPDATED_1,
    UPDATED_2,
    USER,
    duckdb_pipeline,
    issues_updated_at,
    load,
    make_comment,
    make_history,
    make_issue,
    rows_by_id,
)
from tests.utils import load_table_counts, load_tables_to_dicts

FULL_JQL = 'created >= "1970-01-01" ORDER BY updated ASC'


def data_columns(pipeline: dlt.Pipeline, table: str) -> Dict[str, str]:
    columns = pipeline.default_schema.get_table_columns(table)
    return {
        name: columns[name]["data_type"] for name in columns if not name.startswith("_")
    }


def test_issues_load(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_2, labels=[]))
    pipeline = duckdb_pipeline("jira_issues_load")
    load(pipeline, jira_issues(**CREDENTIALS))

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
    assert data_columns(pipeline, "issues") == {
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
        "customfield_10034__name": "text",
        "customfield_10034__group_id": "text",
    }
    rows = rows_by_id(pipeline, "issues")
    assert [row["updated"] for row in rows] == [
        datetime(2024, 1, 15, 9, 30, 0, 123000, tzinfo=timezone.utc),
        datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc),
    ]
    assert rows[0]["key"] == "KAN-1"
    assert rows[0]["description"] == "h1. Title\n*bold*"
    assert rows[0]["duedate"] == date(2024, 2, 1)
    assert rows[0]["status__status_category__key"] == "indeterminate"
    assert rows[0]["customfield_10034__name"] == "admins"
    assert rows[0]["customfield_10050"] == '{"anything":["goes",1]}'


def test_issues_captured(captured_mock: JiraMock) -> None:
    pipeline = duckdb_pipeline("jira_issues_captured")
    load(pipeline, jira_issues(**CREDENTIALS))

    assert load_table_counts(
        pipeline,
        "issues",
        "issues__subtasks",
        "issues__issuelinks",
        "issues__customfield_10020",
    ) == {
        "issues": 8,
        "issues__subtasks": 2,
        "issues__issuelinks": 5,
        "issues__customfield_10020": 1,
    }
    rows = {
        row["key"]: row for row in load_tables_to_dicts(pipeline, "issues")["issues"]
    }
    assert rows["TST-19"]["parent__key"] == "TST-18"
    assert rows["TST-24"]["customfield_10046__child__value"] == "[dummy] rest_api"
    assert rows["TST-24"]["timetracking__original_estimate_seconds"] == 7200
    assert rows["TST-24"]["customfield_10019"] == "0|i0008n:"
    # restrictions have dynamic keys and are not loaded by default
    assert not any(name.startswith("issuerestriction") for name in rows["TST-24"])

    # a page per issue is fetched in parallel, the column hints reach the extractor before any
    # page, so the schema and the rows are the same
    paged = duckdb_pipeline("jira_issues_captured_paged")
    load(paged, jira_issues(**CREDENTIALS, chunk_size=1))
    assert len(captured_mock.bulk_fetches) == 9
    assert data_columns(paged, "issues") == data_columns(pipeline, "issues")

    def comparable(pipeline: dlt.Pipeline) -> List[Dict[str, Any]]:
        return [
            {k: v for k, v in row.items() if not k.startswith("_dlt")}
            for row in rows_by_id(pipeline, "issues")
        ]

    assert comparable(paged) == comparable(pipeline)


def test_issues_pagination(jira_mock: JiraMock) -> None:
    # nothing is fetched for issues, comments and changelogs when search finds no issues
    issues = jira_issues(**CREDENTIALS)
    assert list(issues | issue_changelogs) == []
    assert list(issues | issue_comments) == []
    assert len(jira_mock.searches) == 2
    assert jira_mock.bulk_fetches == []
    assert jira_mock.changelog_fetches == []
    assert jira_mock.comment_fetches == []

    jira_mock.searches.clear()
    jira_mock.add_issues(*issues_updated_at(range(1, 6)))
    jira_mock.search_pages = [["1", "2"], ["3", "4"], ["5"]]
    # ids found by search pages of `search_page_size` are listed in pages of `chunk_size`
    reader = IssuesReader(JiraClient(**CREDENTIALS), chunk_size=3, search_page_size=2)
    assert [len(page) for page in issue_ids(reader)] == [3, 2]
    assert [search.get("nextPageToken") for search in jira_mock.searches] == [
        None,
        "1",
        "2",
    ]
    assert len({search["jql"] for search in jira_mock.searches}) == 1
    assert {search["maxResults"] for search in jira_mock.searches} == {"2"}

    # each page is fetched in batches of `bulk_fetch_size`
    rows = list(
        jira_issues(**CREDENTIALS, chunk_size=3, bulk_fetch_size=2, search_page_size=2)
    )
    assert sorted(row["id"] for row in rows) == ["1", "2", "3", "4", "5"]
    assert sorted(fetch["issueIdsOrKeys"] for fetch in jira_mock.bulk_fetches) == [
        ["1", "2"],
        ["3"],
        ["4", "5"],
    ]


@pytest.mark.parametrize(
    "included_columns,max_bulk_issues,expected_batches",
    [
        (["summary"], None, [50, 1000]),
        (None, None, [50] + [100] * 10),
        (["summary"], 100, [50] + [100] * 10 + [1000]),
    ],
    ids=["single_value_fields", "multi_value_fields", "rejected_batch"],
)
def test_issues_bulk_fetch_batches(
    jira_mock: JiraMock,
    included_columns: Optional[List[str]],
    max_bulk_issues: Optional[int],
    expected_batches: List[int],
) -> None:
    jira_mock.add_issues(*issues_updated_at(range(1, 1051)))
    jira_mock.max_bulk_issues = max_bulk_issues
    rows = list(jira_issues(**CREDENTIALS, included_columns=included_columns))
    assert len(rows) == 1050
    # pages hold 1000 issues, fetched with the largest requests the fields allow
    assert (
        sorted(len(fetch["issueIdsOrKeys"]) for fetch in jira_mock.bulk_fetches)
        == expected_batches
    )


def test_issues_incremental_runs(jira_mock: JiraMock, mocker: MockerFixture) -> None:
    jira_mock.add_issues(
        make_issue(1, "2024-01-15T10:30:00.000+0000"),
        make_issue(2, "2024-01-16T08:00:30.000+0000"),
        make_issue(3, "2024-01-14T09:00:00.000+0000"),
    )
    pipeline = duckdb_pipeline("jira_issues_incremental")
    load(pipeline, jira_issues(**CREDENTIALS))
    table = pipeline.default_schema.get_table("issues")
    assert table["write_disposition"] == "merge"
    assert table["columns"]["id"]["primary_key"] is True
    assert table["columns"]["updated"]["dedup_sort"] == "desc"
    assert [row["id"] for row in rows_by_id(pipeline, "issues")] == ["1", "2", "3"]

    # issue 1 is updated and issue 4 is new. issue 5 falls in the minute the JQL rounds the range
    # down to but before the start value, issue 3 is older than the range
    jira_mock.add_issues(
        make_issue(1, "2024-01-17T12:00:45.000+0000", summary="changed"),
        make_issue(4, "2024-01-17T13:00:00.000+0000"),
        make_issue(5, "2024-01-16T07:50:10.000+0000"),
    )
    load(pipeline, jira_issues(**CREDENTIALS))
    assert jira_mock.jqls == [
        FULL_JQL,
        # the last value minus 10 minutes of lag
        'updated >= "2024-01-16 07:50" ORDER BY updated ASC',
    ]
    assert jira_mock.matching_ids(jira_mock.jqls[1]) == ["5", "2", "1", "4"]
    # issue 2 is read again within lag, issue 5 is dropped by the incremental
    assert pipeline.last_trace.last_normalize_info.row_counts["issues"] == 3
    assert [(row["id"], row["summary"]) for row in rows_by_id(pipeline, "issues")] == [
        ("1", "changed"),
        ("2", "Issue 2"),
        ("3", "Issue 3"),
        ("4", "Issue 4"),
    ]

    # issue 4 is found by two search pages and updated between their fetches, issue 9 is deleted
    # after the search listed it
    jira_mock.add_issues(make_issue(9, "2024-01-18T10:00:00.000+0000"))
    jira_mock.missing_issues = {"9"}
    jira_mock.search_pages = [["4", "9"], ["4"]]
    bulk_fetch = JiraClient.bulk_fetch

    def updated_between_fetches(
        client: JiraClient, ids: List[str], fields: List[str], batch_size: int
    ) -> Any:
        issues = bulk_fetch(client, ids, fields, batch_size)
        if ids == ["4"]:
            for issue in issues:
                issue["fields"].update(
                    summary="newest", updated="2024-01-18T11:00:00.000+0000"
                )
        return issues

    mocker.patch.object(JiraClient, "bulk_fetch", updated_between_fetches)
    load(pipeline, jira_issues(**CREDENTIALS, chunk_size=2))
    assert pipeline.last_trace.last_normalize_info.row_counts["issues"] == 2
    rows = rows_by_id(pipeline, "issues")
    assert [row["id"] for row in rows] == ["1", "2", "3", "4"]
    # the version with the latest update wins the merge
    assert rows[3]["summary"] == "newest"

    # nothing changed since: the cursor advanced to the update of the deleted issue, the range
    # lists no issue and nothing is extracted
    del jira_mock.issues["9"]
    jira_mock.missing_issues.clear()
    jira_mock.search_pages = None
    jira_mock.bulk_fetches.clear()
    pipeline.run(jira_issues(**CREDENTIALS))
    assert jira_mock.jqls[-1] == 'updated >= "2024-01-18 09:50" ORDER BY updated ASC'
    assert jira_mock.bulk_fetches == []
    assert "issues" not in pipeline.last_trace.last_normalize_info.row_counts
    assert [(row["id"], row["summary"]) for row in rows_by_id(pipeline, "issues")] == [
        ("1", "changed"),
        ("2", "Issue 2"),
        ("3", "Issue 3"),
        ("4", "newest"),
    ]


def test_issues_state_reset(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_2))
    pipeline = duckdb_pipeline("jira_issues_state")
    load(pipeline, jira_issues(**CREDENTIALS))
    incremental_jql = 'updated >= "2024-01-16 15:50" ORDER BY updated ASC'

    # the state of the parent resource is dropped with the table
    load(pipeline, jira_issues(**CREDENTIALS), refresh="drop_resources")
    load(pipeline, jira_issues(**CREDENTIALS))
    assert jira_mock.jqls == [FULL_JQL, FULL_JQL, incremental_jql]
    assert load_table_counts(pipeline, "issues") == {"issues": 2}

    # the drop command matches exact names, the parent must be named too
    pipeline_drop(pipeline, resources=["issues"])()
    load(pipeline, jira_issues(**CREDENTIALS))
    assert jira_mock.jqls[-1] == incremental_jql
    pipeline_drop(pipeline, resources=["issues", "issues_ids"])()
    load(pipeline, jira_issues(**CREDENTIALS))
    assert jira_mock.jqls[-1] == FULL_JQL


def test_issues_upgrade_from_replace(jira_mock: JiraMock) -> None:
    @dlt.resource(name="issues", write_disposition="replace", max_table_nesting=2)
    def replaced_issues() -> Any:
        yield {"id": "1", "fields": {"labels": ["a", "b"]}}

    pipeline = duckdb_pipeline("jira_issues_upgrade")
    load(pipeline, replaced_issues)
    assert "issues__fields__labels" in pipeline.default_schema.tables

    # the nested tables of the replaced issues have no root key for the merge
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    with pytest.raises(PipelineStepFailed) as py_ex:
        pipeline.run(jira_issues(**CREDENTIALS))
    assert py_ex.value.step == "load"
    assert "_dlt_root_id" in str(py_ex.value)

    # the recovery of the changelog: abort the package, then refresh
    pipeline.drop_pending_packages()
    load(pipeline, jira_issues(**CREDENTIALS), refresh="drop_resources")
    assert load_table_counts(pipeline, "issues", "issues__labels") == {
        "issues": 1,
        "issues__labels": 2,
    }


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
    rows = list(jira_issues(**CREDENTIALS))
    # empty fields are skipped
    assert [row.get("project") for row in rows] == [None, None, rows[2]["project"]]
    assert rows[2]["project"] is not None
    # values of unexpected structure are loaded as null
    assert rows[2].get("customfield_10030") is None
    assert rows[2].get("customfield_10016") is None
    assert rows[2].get("customfield_10031") is None
    assert rows[0]["customfield_10030"] is not None


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
    rows = list(
        jira_issues(
            **CREDENTIALS,
            included_columns=included_columns,
            excluded_columns=excluded_columns,
        )
    )
    assert jira_mock.bulk_fetches[0]["fields"] == expected_fields
    # the issue has no thumbnail and no parent, empty fields are skipped
    present = [f for f in expected_fields if f not in ("thumbnail", "parent")]
    assert list(rows[0]) == ["id", "key", *present]


def test_issues_user_column_hints(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    issues = jira_issues(**CREDENTIALS)
    issues.apply_hints(columns={"customfield_10016": {"data_type": "text"}})
    pipeline = duckdb_pipeline("jira_issues_hints")
    load(pipeline, issues)
    columns = pipeline.default_schema.get_table_columns("issues")
    assert columns["customfield_10016"]["data_type"] == "text"
    assert columns["updated"]["data_type"] == "timestamp"


@pytest.mark.parametrize(
    "resources,expected_fields",
    [
        (["issues"], [DEFAULT_FIELD_IDS]),
        (["issue_comments"], [DEFAULT_FIELD_IDS, ["comment"]]),
        (
            ["issues", "issue_comments", "issue_changelogs"],
            [DEFAULT_FIELD_IDS, ["comment"]],
        ),
    ],
    ids=["issues", "comments", "all"],
)
def test_issues_fetch_selected_resources(
    jira_mock: JiraMock, resources: List[str], expected_fields: List[List[str]]
) -> None:
    jira_mock.add_issues(*issues_updated_at((1, 2)))
    jira_mock.comments = {"1": [make_comment(10, UPDATED_1)]}
    jira_mock.histories = {"2": [make_history(200, UPDATED_1)]}
    pipeline = duckdb_pipeline("jira_selected_resources")
    load(pipeline, jira(**CREDENTIALS).with_resources(*resources))

    # issues are fetched for their transformers even when they are not loaded
    assert [fetch["fields"] for fetch in jira_mock.bulk_fetches] == expected_fields
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert {name for name in row_counts if "__" not in name} == {
        "_dlt_pipeline_state",
        *resources,
    }


def test_changelog_rows(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(*issues_updated_at((1, 2, 3)))
    jira_mock.histories = {
        "1": [make_history(history_id, UPDATED_1) for history_id in (100, 101, 102)],
        "3": [
            make_history(300, UPDATED_2, field="assignee"),
            make_history(301, UPDATED_2, as_text=True),
        ],
    }
    jira_mock.changelog_page_size = 2
    issues = jira_issues(**CREDENTIALS)
    pipeline = duckdb_pipeline("jira_changelogs")
    load(pipeline, [issues, issues | issue_changelogs])

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
    rows = rows_by_id(pipeline, "issue_changelogs")
    assert [(row["id"], row["issue_id"]) for row in rows] == [
        ("100", "1"),
        ("101", "1"),
        ("102", "1"),
        ("300", "3"),
        ("301", "3"),
    ]
    assert rows[3]["author__account_id"] == "a1"
    # `created` comes as UNIX milliseconds or as text
    assert (
        rows[3]["created"]
        == rows[4]["created"]
        == datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc)
    )

    # changes of more than 1000 issues are fetched by 1000 ids and yielded in pages of `chunk_size`
    jira_mock.changelog_fetches.clear()
    jira_mock.histories = {
        "1": [make_history(history_id, UPDATED_1) for history_id in range(5)]
    }
    issue_rows = [{"id": str(issue_id)} for issue_id in range(1001)]
    pages = list(changelog_rows(JiraClient(**CREDENTIALS), issue_rows, chunk_size=3))
    assert [len(page) for page in pages] == [3, 2]
    assert [len(fetch["issueIdsOrKeys"]) for fetch in jira_mock.changelog_fetches] == [
        1000,
        1000,
        1000,
        1,
    ]
