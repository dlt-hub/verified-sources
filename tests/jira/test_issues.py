import os
from datetime import date, datetime, timezone
from time import sleep
from typing import Any, Dict, List, Optional, Sequence

import dlt
import pytest
from dlt.common.time import ensure_datetime_in_tz
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.pipeline.helpers import pipeline_drop
from pytest_mock import MockerFixture

from sources.jira import (
    drop_keys,
    issue_changelogs,
    issue_comments,
    jira,
    jira_issues,
)
from sources.jira.helpers.client import JiraClient
from sources.jira.helpers.issues import changelog_rows

from tests.jira.utils import (
    ALL_FIELD_IDS,
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
    # fields without values get no columns
    jira_mock.add_issues(
        make_issue(1, UPDATED_1, customfield_10032=None),
        make_issue(2, UPDATED_2, labels=[], customfield_10032=None),
    )
    pipeline = duckdb_pipeline("jira_issues_load")
    load(pipeline, jira(**CREDENTIALS).with_resources("issues"))

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
        "customfield_10033": "timestamp",
        # values of unknown structure are text
        "customfield_10050": "text",
        # objects are flattened as Jira returns them, with REST links and image urls
        "status__self": "text",
        "status__id": "text",
        "status__name": "text",
        "status__icon_url": "text",
        "status__status_category__id": "bigint",
        "status__status_category__key": "text",
        "status__status_category__name": "text",
        "project__id": "text",
        "project__key": "text",
        "project__name": "text",
        "assignee__self": "text",
        "assignee__account_id": "text",
        "assignee__display_name": "text",
        "assignee__active": "bool",
        "assignee__account_type": "text",
        "assignee__avatar_urls___16x16": "text",
        "assignee__time_zone": "text",
        "customfield_10030__self": "text",
        "customfield_10030__id": "text",
        "customfield_10030__value": "text",
        "customfield_10034__self": "text",
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
        "issues__customfield_10020",
    ) == {
        "issues": 8,
        "issues__subtasks": 2,
        "issues__customfield_10020": 1,
    }
    rows = {
        row["key"]: row for row in load_tables_to_dicts(pipeline, "issues")["issues"]
    }
    # links, attachments, votes and watchers are not loaded by default
    tables = {table["name"] for table in pipeline.default_schema.data_tables()}
    assert not tables & {"issues__issuelinks", "issues__attachment"}
    assert not any(name.startswith(("votes", "watches")) for name in rows["TST-24"])

    assert rows["TST-19"]["parent__key"] == "TST-18"
    assert rows["TST-24"]["customfield_10046__child__value"] == "[dummy] rest_api"
    assert rows["TST-24"]["timetracking__original_estimate_seconds"] == 7200
    assert rows["TST-24"]["customfield_10019"] == "0|i0008n:"
    # restrictions have dynamic keys and are not loaded by default
    assert not any(name.startswith("issuerestriction") for name in rows["TST-24"])

    # a page per issue is fetched in parallel and pages reach the extractor in any order, the
    # schema and the rows are the same
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

    # `excluded_columns=[]` loads all fields
    everything = duckdb_pipeline("jira_issues_captured_all_fields")
    load(everything, jira_issues(**CREDENTIALS, excluded_columns=[]))
    assert load_table_counts(
        everything, "issues", "issues__issuelinks", "issues__attachment"
    ) == {"issues": 8, "issues__issuelinks": 5, "issues__attachment": 1}
    everything_row = rows_by_id(everything, "issues")[0]
    assert any(name.startswith("votes") for name in everything_row)
    assert any(name.startswith("watches") for name in everything_row)


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
    # ids found by search pages of `search_page_size` are fetched in pages of `chunk_size`
    assert len(list(jira_issues(**CREDENTIALS, chunk_size=3, search_page_size=2))) == 5
    assert sorted(len(f["issueIdsOrKeys"]) for f in jira_mock.bulk_fetches) == [2, 3]
    assert [search.get("nextPageToken") for search in jira_mock.searches] == [
        None,
        "1",
        "2",
    ]
    assert len({search["jql"] for search in jira_mock.searches}) == 1
    assert {search["maxResults"] for search in jira_mock.searches} == {"2"}
    jira_mock.bulk_fetches.clear()

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
    "columns,max_bulk_issues,expected_batches",
    [
        ({"included_columns": ["summary"]}, None, [50, 1000]),
        ({"included_columns": ["summary", "labels"]}, None, [50, 1000]),
        ({}, None, [50, 1000]),
        ({"excluded_columns": []}, None, [50] + [100] * 10),
        ({"included_columns": ["summary", "attachment"]}, None, [50] + [100] * 10),
        ({"included_columns": ["summary"]}, 100, [50] + [100] * 10 + [1000]),
    ],
    ids=[
        "included_fields",
        "multi_value_field",
        "default_fields",
        "all_fields",
        "capped_field",
        "rejected_batch",
    ],
)
def test_issues_bulk_fetch_batches(
    jira_mock: JiraMock,
    columns: Dict[str, List[str]],
    max_bulk_issues: Optional[int],
    expected_batches: List[int],
) -> None:
    jira_mock.add_issues(*issues_updated_at(range(1, 1051)))
    jira_mock.max_bulk_issues = max_bulk_issues
    rows = list(jira_issues(**CREDENTIALS, **columns))
    assert len(rows) == 1050
    # search lists only ids, so one page holds all of them
    assert len(jira_mock.searches) == 1
    # pages hold 1000 issues, fetched with the largest requests the fields allow
    assert (
        sorted(len(fetch["issueIdsOrKeys"]) for fetch in jira_mock.bulk_fetches)
        == expected_batches
    )


@pytest.mark.parametrize("search_page_size", [5000, 2], ids=["one_page", "pages_of_2"])
def test_issues_incremental_runs(
    jira_mock: JiraMock, mocker: MockerFixture, search_page_size: int
) -> None:
    jira_mock.add_issues(
        make_issue(1, "2024-01-15T10:30:00.000+0000"),
        make_issue(2, "2024-01-16T08:00:30.000+0000"),
        make_issue(3, "2024-01-14T09:00:00.000+0000"),
    )
    pipeline = duckdb_pipeline("jira_issues_incremental")
    load(pipeline, jira_issues(**CREDENTIALS, search_page_size=search_page_size))
    table = pipeline.default_schema.get_table("issues")
    assert table["write_disposition"] == "merge"
    assert table["columns"]["id"]["primary_key"] is True
    assert table["columns"]["updated"]["dedup_sort"] == "desc"
    assert [row["id"] for row in rows_by_id(pipeline, "issues")] == ["1", "2", "3"]

    # issue 1 is updated, issue 4 is new, issue 5 is in the minute of the JQL range but before the
    # start value, issue 3 is older than the range
    jira_mock.add_issues(
        make_issue(1, "2024-01-17T12:00:45.000+0000", summary="changed"),
        make_issue(4, "2024-01-17T13:00:00.000+0000"),
        make_issue(5, "2024-01-16T07:50:10.000+0000"),
    )
    load(pipeline, jira_issues(**CREDENTIALS, search_page_size=search_page_size))
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
    load(
        pipeline,
        jira_issues(**CREDENTIALS, chunk_size=2, search_page_size=search_page_size),
    )
    assert pipeline.last_trace.last_normalize_info.row_counts["issues"] == 2
    rows = rows_by_id(pipeline, "issues")
    assert [row["id"] for row in rows] == ["1", "2", "3", "4"]
    # the version with the latest update wins the merge
    assert rows[3]["summary"] == "newest"

    # nothing changed since: the cursor advanced to the latest fetched update, not to the update
    # of the deleted issue listed by search. The range lists no issue and nothing is extracted
    del jira_mock.issues["9"]
    jira_mock.missing_issues.clear()
    jira_mock.search_pages = None
    jira_mock.bulk_fetches.clear()
    pipeline.run(jira_issues(**CREDENTIALS, search_page_size=search_page_size))
    assert jira_mock.jqls[-1] == 'updated >= "2024-01-18 10:50" ORDER BY updated ASC'
    assert jira_mock.bulk_fetches == []
    # cursor values come from fetched issues, so search lists only ids
    assert {search["fields"] for search in jira_mock.searches} == {"id"}
    assert {search["maxResults"] for search in jira_mock.searches} == {
        str(search_page_size)
    }
    assert "issues" not in pipeline.last_trace.last_normalize_info.row_counts
    assert [(row["id"], row["summary"]) for row in rows_by_id(pipeline, "issues")] == [
        ("1", "changed"),
        ("2", "Issue 2"),
        ("3", "Issue 3"),
        ("4", "newest"),
    ]


# issue n updated on day n of January, chunks of 3 and a partial one
CHUNKED_UPDATES = [f"2024-01-{day:02d}T10:00:00.000+0000" for day in range(1, 11)]


def first_chunk_finishes_last(mocker: MockerFixture) -> None:
    bulk_fetch = JiraClient.bulk_fetch

    def slow_first_chunk(
        client: JiraClient, ids: List[str], fields: List[str], batch_size: int
    ) -> Any:
        if "1" in ids:
            sleep(0.5)
        return bulk_fetch(client, ids, fields, batch_size)

    mocker.patch.object(JiraClient, "bulk_fetch", slow_first_chunk)


def chunked_issues() -> Any:
    return jira_issues(
        **CREDENTIALS,
        chunk_size=3,
        incremental=dlt.sources.incremental(
            "updated", row_order="asc", range_start="open"
        ),
    )


@pytest.mark.parametrize(
    "limit_on", ["issues", "ids", "source", "count_rows"], ids=lambda value: value
)
def test_issues_chunked_runs(
    jira_mock: JiraMock, mocker: MockerFixture, limit_on: str
) -> None:
    jira_mock.add_issues(
        *(make_issue(n, updated) for n, updated in enumerate(CHUNKED_UPDATES, 1))
    )
    # parallel fetches of all chunks would let the short last chunk finish first
    first_chunk_finishes_last(mocker)
    os.environ["EXTRACT__WORKERS"] = "5"
    pipeline = duckdb_pipeline("jira_issues_chunked")

    def limited() -> Any:
        issues = chunked_issues()
        if limit_on == "issues":
            return issues.add_limit(1)
        if limit_on == "ids":
            issues._parent.add_limit(1)
            return issues
        if limit_on == "count_rows":
            return issues.add_limit(3, count_rows=True)
        return dlt.source(lambda: issues, name="jira")().add_limit(1)

    # JQL compares whole minutes: an open range lists the issue of the last value again and the
    # incremental drops it, so a run after the first loads one issue less than the chunk
    chunks = [["1", "2", "3"], ["4", "5"], ["6", "7"], ["8", "9"], ["10"]]
    for chunk in chunks:
        jira_mock.searches.clear()
        load(pipeline, limited())
        assert pipeline.last_trace.last_normalize_info.row_counts["issues"] == len(
            chunk
        )
        loaded = sorted((row["id"] for row in rows_by_id(pipeline, "issues")), key=int)
        assert loaded == [str(n) for n in range(1, int(chunk[-1]) + 1)]
        # the search lists only the issues of the limited page
        assert {search["maxResults"] for search in jira_mock.searches} == {"3"}
        state = next(
            source["resources"]["issues_ids"]["incremental"]["updated"]
            for source in pipeline.state["sources"].values()
        )
        assert state["last_value"] == ensure_datetime_in_tz(
            CHUNKED_UPDATES[int(chunk[-1]) - 1]
        )
    pipeline.run(limited())
    assert "issues" not in pipeline.last_trace.last_normalize_info.row_counts


def test_issues_time_limit_fetches_in_order(
    jira_mock: JiraMock, mocker: MockerFixture
) -> None:
    jira_mock.add_issues(
        *(make_issue(n, updated) for n, updated in enumerate(CHUNKED_UPDATES, 1))
    )
    first_chunk_finishes_last(mocker)
    os.environ["EXTRACT__WORKERS"] = "5"
    pipeline = duckdb_pipeline("jira_issues_time_limit")
    # a time limit cannot be pushed down to the search, pages are fetched one after another
    load(pipeline, chunked_issues().add_limit(max_time=60))
    assert [fetch["issueIdsOrKeys"] for fetch in jira_mock.bulk_fetches] == [
        ["1", "2", "3"],
        ["4", "5", "6"],
        ["7", "8", "9"],
        ["10"],
    ]
    assert len(rows_by_id(pipeline, "issues")) == len(CHUNKED_UPDATES)


def test_issues_updated_during_run(jira_mock: JiraMock, mocker: MockerFixture) -> None:
    jira_mock.add_issues(
        make_issue(1, "2024-01-15T10:30:00.000+0000"),
        make_issue(2, "2024-01-16T08:00:00.000+0000"),
    )
    bulk_fetch = JiraClient.bulk_fetch

    def updated_after_start(
        client: JiraClient, ids: List[str], fields: List[str], batch_size: int
    ) -> Any:
        issues = bulk_fetch(client, ids, fields, batch_size)
        for issue in issues:
            if issue["id"] == "2":
                issue["fields"].update(
                    summary="during run", updated="2099-01-01T00:00:00.000+0000"
                )
        return issues

    mocker.patch.object(JiraClient, "bulk_fetch", updated_after_start)
    # a replace load keeps no state, so it loads every issue it fetched
    rows = list(jira_issues(**CREDENTIALS, write_disposition="replace"))
    assert sorted(row["id"] for row in rows) == ["1", "2"]

    # issue 2 is updated after the run started, so the next run loads it and the cursor does
    # not move past updates that search did not list
    pipeline = duckdb_pipeline("jira_issues_during_run")
    load(pipeline, jira_issues(**CREDENTIALS))
    assert [row["id"] for row in rows_by_id(pipeline, "issues")] == ["1"]

    mocker.patch.object(JiraClient, "bulk_fetch", bulk_fetch)
    jira_mock.add_issues(
        make_issue(2, "2024-01-17T00:00:00.000+0000", summary="during run")
    )
    load(pipeline, jira_issues(**CREDENTIALS))
    assert jira_mock.jqls[-1] == 'updated >= "2024-01-15 10:20" ORDER BY updated ASC'
    assert [(row["id"], row["summary"]) for row in rows_by_id(pipeline, "issues")] == [
        ("1", "Issue 1"),
        ("2", "during run"),
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

    # the upgrade path in the README changelog: abort the package, then refresh
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
            timespent=1.5,
        ),
        make_issue(
            4, UPDATED_1, customfield_10016=3, timespent=7200.0, customfield_10050=[]
        ),
    )
    rows = list(jira_issues(**CREDENTIALS))
    # empty fields are skipped
    assert [row.get("project") for row in rows[:3]] == [None, None, rows[2]["project"]]
    assert rows[2]["project"] is not None
    # values of unexpected structure or not convertible to the field type are loaded as null
    assert rows[2].get("customfield_10030") is None
    assert rows[2].get("customfield_10016") is None
    assert rows[2].get("customfield_10031") is None
    assert rows[2].get("timespent") is None
    assert rows[0]["customfield_10030"] is not None
    # values are converted to the field type, so an integer does not create a variant column
    assert type(rows[3]["customfield_10016"]) is float
    assert type(rows[3]["timespent"]) is int
    # empty values of unknown structure are skipped, not loaded as "[]"
    assert "customfield_10050" not in rows[3]


@pytest.mark.parametrize(
    "columns,expected_fields",
    [
        (
            {"included_columns": ["summary", "status", "colour"]},
            ["summary", "status", "updated"],
        ),
        (
            {"included_columns": ["summary", "updated", "comment"]},
            ["summary", "updated", "comment"],
        ),
        # the default excluded columns do not filter the included ones
        (
            {"included_columns": ["summary", "attachment"]},
            ["summary", "attachment", "updated"],
        ),
        ({}, DEFAULT_FIELD_IDS),
        ({"excluded_columns": []}, ALL_FIELD_IDS),
        (
            {"excluded_columns": ("description", "customfield_10050")},
            [
                field_id
                for field_id in ALL_FIELD_IDS
                if field_id not in ("description", "customfield_10050")
            ],
        ),
    ],
    ids=[
        "included",
        "included_comments",
        "included_default_excluded",
        "default",
        "all_fields",
        "excluded",
    ],
)
def test_issues_columns(
    jira_mock: JiraMock,
    columns: Dict[str, Sequence[str]],
    expected_fields: List[str],
) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    rows = list(jira_issues(**CREDENTIALS, **columns))
    assert jira_mock.bulk_fetches[0]["fields"] == expected_fields
    # the issue has no thumbnail and no parent, empty fields are skipped
    present = [f for f in expected_fields if f not in ("thumbnail", "parent")]
    assert list(rows[0]) == ["id", "key", *present]


@pytest.mark.parametrize(
    "keys,dropped,kept",
    [
        (
            None,
            [
                "issues.assignee__self",
                "issues.assignee__avatar_urls___16x16",
                "issues.status__icon_url",
                "issues__fix_versions.self",
                "issue_comments.self",
                "issue_comments.author__self",
            ],
            ["issues.assignee__time_zone"],
        ),
        (
            ["timeZone"],
            ["issues.assignee__time_zone", "issue_comments.author__time_zone"],
            ["issues.assignee__self", "issues__fix_versions.self"],
        ),
    ],
    ids=["urls", "other_keys"],
)
def test_drop_keys(
    jira_mock: JiraMock,
    keys: Optional[List[str]],
    dropped: List[str],
    kept: List[str],
) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    jira_mock.comments = {"1": [make_comment(10, UPDATED_1)]}
    drop = drop_keys() if keys is None else drop_keys(keys)
    issues = jira_issues(**CREDENTIALS).add_map(drop)
    pipeline = duckdb_pipeline("jira_drop_keys")
    # transformers piped from a mapped resource still receive the pages of issues
    load(pipeline, [issues, (issues | issue_comments).add_map(drop)])
    assert load_table_counts(pipeline, "issues", "issue_comments") == {
        "issues": 1,
        "issue_comments": 1,
    }
    columns = {
        f"{table['name']}.{name}"
        for table in pipeline.default_schema.data_tables()
        for name in table["columns"]
    }
    assert not set(dropped) & columns
    assert set(kept) <= columns


def test_issues_included_column_hints(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    issues = jira_issues(
        **CREDENTIALS, included_columns=["customfield_10016", "thumbnail", "duedate"]
    )
    issues.apply_hints(columns={"customfield_10016": {"data_type": "text"}})
    pipeline = duckdb_pipeline("jira_issues_hints")
    load(pipeline, issues)
    # included fields get typed columns also without values, the hints of the user win
    assert data_columns(pipeline, "issues") == {
        "id": "text",
        "key": "text",
        "customfield_10016": "text",
        "thumbnail": "text",
        "duedate": "date",
        "updated": "timestamp",
    }


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
