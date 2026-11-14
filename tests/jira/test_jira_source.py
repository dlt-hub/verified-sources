import os
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import dlt
import pytest
from dlt.common.time import ensure_datetime_in_tz
from dlt.sources.helpers.requests import HTTPError
from pytest_mock import MockerFixture

from sources.jira import (
    issue_changelogs,
    issue_comments,
    jira,
    jira_issues,
    worklogs,
)
from sources.jira.helpers.client import JiraClient
from sources.jira.settings import DEFAULT_ENDPOINTS, DEFAULT_LAG

from tests.jira.seed_test_data import (
    ATTACHMENT_FILENAME,
    CHANGELOGS_PER_CASE,
    COMMENTS_PER_CASE,
    SUMMARIES_PER_CASE,
    WORKLOGS_PER_CASE,
    case_label,
    expected_issues,
    issues_of_case,
    request,
    search_ids,
    seeded_issues,
)
from tests.jira.utils import (
    SEED_LABEL,
    filesystem_pipeline,
    page_sizes,
    record_search_pages,
)
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    load_table_distinct_counts,
    load_tables_to_dicts,
)

# pages per resource, so the run stays bounded on a site of any size
PAGE_LIMIT = 2
# much less than the seeded issues, so the runs read several search pages
SEARCH_PAGE_SIZE = 100


@pytest.fixture(autouse=True)
def jira_credentials() -> None:
    """Skips the tests without Jira credentials, fails them in CI"""
    subdomain = dlt.secrets.get("sources.jira.subdomain")
    if subdomain and subdomain != "set me up":
        return
    if os.environ.get("CI"):
        pytest.fail(
            "[sources.jira] is missing from DLT_SECRETS_TOML, so CI tested nothing"
        )
    pytest.skip("set [sources.jira] in sources/.dlt/secrets.toml to run these")


def seeded_issues_with_jql() -> Tuple[List[Dict[str, Any]], str]:
    """Returns the seeded issues and the JQL that finds them, without the issue that
    `test_issue_comments_changed` writes to
    """
    excluded = ("comment_changes",)
    seeded = seeded_issues(excluded)
    assert len(seeded) == expected_issues(excluded)
    return seeded, f'labels = "{SEED_LABEL}" AND NOT labels = "{case_label(*excluded)}"'


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_endpoints(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    # seeded issues with comments, changes and worklogs. `add_limit` bounds the issues to
    # `PAGE_LIMIT` pages of 1000, more than the seeded issues
    all_endpoints = jira(
        page_size=5,
        jql=f'labels = "{SEED_LABEL}" AND NOT labels = "{SEED_LABEL}-bulk-issues"',
    )
    # users are tested by `test_load_users`
    all_endpoints.users.selected = False
    all_endpoints.max_table_nesting = 0
    all_endpoints.add_limit(PAGE_LIMIT)
    info = pipeline.run(all_endpoints)
    assert_load_info(info)
    counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    keys = {
        "issues": "id",
        "issue_comments": "id",
        "issue_changelogs": "id",
        "worklogs": "id",
        "issue_fields": "id",
        **{
            name: endpoint["primary_key"]
            for name, endpoint in DEFAULT_ENDPOINTS.items()
        },
    }
    assert set(counts) == set(keys) - {"users"}
    assert all(count > 0 for count in counts.values())
    # the key each resource declares is unique in its table
    for table, count in counts.items():
        assert load_table_distinct_counts(pipeline, keys[table], table) == {
            table: count
        }


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_users(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    source = jira(page_size=5)
    source.max_table_nesting = 0
    source = source.with_resources("users")
    source.add_limit(PAGE_LIMIT)
    info = pipeline.run(source)
    assert_load_info(info)
    data_tables = pipeline.default_schema.data_tables()
    assert [table["name"] for table in data_tables] == ["users"]
    counts = load_table_counts(pipeline, "users")
    assert counts == load_table_distinct_counts(pipeline, "account_id", "users")
    assert counts["users"] > 0


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_query_issues(destination_name: str) -> None:
    queries = {
        "seeded": f'labels = "{SEED_LABEL}"',
        "everything": "created >= '2000-01-01'",
    }
    pipeline = dlt.pipeline(
        pipeline_name="test_jira_queries",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    resources = []
    for name, jql in queries.items():
        # each query is limited on its own, so the second one runs and the overlap is real
        issues = jira_issues(jql=jql, name=name, chunk_size=5).add_limit(PAGE_LIMIT)
        issues.max_table_nesting = 0
        resources.append(issues)
    assert_load_info(pipeline.run(resources))
    assert load_table_counts(pipeline, *queries) == {name: 10 for name in queries}
    seeded_ids = load_table_distinct_counts(pipeline, "id", "seeded")
    assert seeded_ids == {"seeded": 10}


def test_search_pages(mocker: MockerFixture) -> None:
    """Reads all seeded issues in search pages of 100, fetched in pages of 200 with the largest
    bulk fetch requests the included fields allow
    """
    seeded = seeded_issues()
    search_pages = record_search_pages(mocker)
    bulk_fetch = mocker.spy(JiraClient, "bulk_fetch")
    issues = jira_issues(
        jql=f'labels = "{SEED_LABEL}"',
        search_page_size=SEARCH_PAGE_SIZE,
        chunk_size=200,
        included_columns=["summary", "labels"],
    )
    rows = list(issues)
    assert sorted(row["id"] for row in rows) == sorted(str(i["id"]) for i in seeded)
    assert {key for row in rows for key in row} == {
        "id",
        "key",
        "summary",
        "labels",
        "updated",
    }
    assert search_pages == page_sizes(len(seeded), SEARCH_PAGE_SIZE)
    fetches = [call.args for call in bulk_fetch.call_args_list]
    assert sorted(len(ids) for _, ids, _, _ in fetches) == sorted(
        page_sizes(len(seeded), 200)
    )
    assert {batch_size for _, _, _, batch_size in fetches} == {1000}


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_workflow_statuses_resolve_the_child_table_references(
    destination_name: str,
) -> None:
    """`workflows__statuses` carries a reference and no name. This is the join that resolves it."""
    pipeline = dlt.pipeline(
        pipeline_name="test_jira_workflow_statuses",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    source = jira(page_size=5).with_resources("workflows", "workflow_statuses")
    source.add_limit(PAGE_LIMIT)
    info = pipeline.run(source)
    assert_load_info(info)

    tables = load_tables_to_dicts(pipeline, "workflows__statuses", "workflow_statuses")
    referenced = {row["status_reference"] for row in tables["workflows__statuses"]}
    resolved = {row["status_reference"] for row in tables["workflow_statuses"]}
    assert referenced
    assert referenced <= resolved
    # the resource deduplicates, so the join stays one to one
    assert len(resolved) == len(tables["workflow_statuses"])
    assert all(row["name"] for row in tables["workflow_statuses"])


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_issue_comments_changed(destination_name: str) -> None:
    """Adds, edits and deletes comments of a seeded issue between loads. Comments of other runs
    on the same issue are ignored.
    """
    client = JiraClient(
        dlt.secrets["sources.jira.subdomain"],
        dlt.secrets["sources.jira.email"],
        dlt.secrets["sources.jira.api_token"],
    )
    issue_ids = search_ids(client, f'labels = "{SEED_LABEL}-comment-changes"')
    if not issue_ids:
        pytest.skip(
            "seed the test site: seed_test_data.py --apply --cases comment_changes"
        )
    path = f"issue/{issue_ids[0]}/comment"
    created: List[str] = [
        request(client, "POST", path, json={"body": f"online test {n}"})["id"]
        for n in (1, 2)
    ]
    pipeline = dlt.pipeline(
        pipeline_name="test_jira_comments_changed",
        destination=destination_name,
        dataset_name="test_jira_comments_changed",
        dev_mode=True,
    )

    def load() -> Dict[str, str]:
        issues = jira_issues(jql=f"id = {issue_ids[0]}")
        assert_load_info(pipeline.run([issues, issues | issue_comments]))
        with pipeline.sql_client() as sql_client:
            rows = sql_client.execute_sql("SELECT id, body FROM issue_comments")
        return {comment_id: body for comment_id, body in rows if comment_id in created}

    try:
        assert load() == {created[0]: "online test 1", created[1]: "online test 2"}
        request(client, "PUT", f"{path}/{created[0]}", json={"body": "edited"})
        request(client, "DELETE", f"{path}/{created[1]}")
        # the issue was updated within lag and is read again
        assert load() == {created[0]: "edited"}
        request(client, "DELETE", f"{path}/{created[0]}")
        assert load() == {}
    finally:
        for comment_id in created:
            try:
                request(client, "DELETE", f"{path}/{comment_id}")
            except HTTPError:
                pass


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_seeded_issues_full_and_incremental(
    destination_name: str, mocker: MockerFixture
) -> None:
    """Loads all seeded issues with their comments, changelogs and worklogs, then in backfill
    windows and incrementally, in search pages of 100. The expected counts and values come from
    the seed cases and the manifest of seeded issues.
    """
    seeded, jql = seeded_issues_with_jql()
    search_pages = record_search_pages(mocker)

    def case_id(case: str) -> str:
        (issue,) = issues_of_case(seeded, case)
        return str(issue["id"])

    # search serves all ids in one page when it returns no fields
    rows = list(jira_issues(jql=jql, included_columns=["summary"]))
    assert len(rows) == len(seeded)
    assert search_pages == [len(seeded)]
    search_pages.clear()

    pipeline = dlt.pipeline(
        pipeline_name="test_jira_seeded",
        destination=destination_name,
        dataset_name="test_jira_seeded",
        dev_mode=True,
    )
    # all fields, so the seeded attachment is loaded
    issues = jira_issues(
        jql=jql, search_page_size=SEARCH_PAGE_SIZE, excluded_columns=[]
    )
    info = pipeline.run(
        [
            issues,
            issues | issue_comments,
            issues | issue_changelogs,
            worklogs(),
        ]
    )
    assert_load_info(info)
    assert search_pages == page_sizes(len(seeded), SEARCH_PAGE_SIZE)
    assert load_table_counts(pipeline, "issues") == {"issues": len(seeded)}
    tables = load_tables_to_dicts(
        pipeline,
        "issues",
        "issues__attachment",
        "issue_comments",
        "issue_changelogs",
        "worklogs",
    )
    rows = {row["id"]: row for row in tables["issues"]}
    assert set(rows) == {str(issue["id"]) for issue in seeded}
    for case, summary in SUMMARIES_PER_CASE.items():
        assert rows[case_id(case)]["summary"] == summary
    assert [row["filename"] for row in tables["issues__attachment"]] == [
        ATTACHMENT_FILENAME
    ]
    # comments, changes and worklogs by the issue they belong to, worklogs are site wide
    assert Counter(row["issue_id"] for row in tables["issue_comments"]) == {
        case_id(case): count for case, count in COMMENTS_PER_CASE.items()
    }
    changelogs = Counter(row["issue_id"] for row in tables["issue_changelogs"])
    assert {
        case_id(case): changelogs[case_id(case)] for case in CHANGELOGS_PER_CASE
    } == {case_id(case): count for case, count in CHANGELOGS_PER_CASE.items()}
    assert Counter(
        row["issue_id"] for row in tables["worklogs"] if row["issue_id"] in rows
    ) == {case_id(case): count for case, count in WORKLOGS_PER_CASE.items()}

    # backfill windows over `created`, with the counts expected from the manifest
    created = sorted(ensure_datetime_in_tz(issue["created"]) for issue in seeded)
    cuts = [
        created[0],
        created[len(created) // 3],
        created[2 * len(created) // 3],
        created[-1] + timedelta(seconds=1),
    ]
    backfill = dlt.pipeline(
        pipeline_name="test_jira_seeded_backfill",
        destination=destination_name,
        dataset_name="test_jira_seeded_backfill",
        dev_mode=True,
    )
    for start, end in zip(cuts, cuts[1:]):
        expected = sum(start <= value < end for value in created)
        assert expected > SEARCH_PAGE_SIZE
        window = dlt.sources.incremental("created", initial_value=start, end_value=end)
        search_pages.clear()
        issues = jira_issues(
            jql=jql, incremental=window, search_page_size=SEARCH_PAGE_SIZE
        )
        assert_load_info(backfill.run(issues))
        assert backfill.last_trace.last_normalize_info.row_counts["issues"] == expected
        # JQL compares whole minutes, so search lists more issues than the window holds
        assert sum(search_pages) >= expected
        assert search_pages == page_sizes(sum(search_pages), SEARCH_PAGE_SIZE)
    assert load_table_counts(backfill, "issues") == {"issues": len(seeded)}

    # runs without changes: issues within lag are read again, with no lag the issues updated last
    # once more, then none
    updated = [ensure_datetime_in_tz(issue["updated"]) for issue in seeded]
    last = max(updated)
    within_lag = sum(
        value >= last - timedelta(seconds=DEFAULT_LAG) for value in updated
    )
    at_last = sum(value == last for value in updated)
    for lag, expected in ((None, within_lag), (0, at_last), (0, 0)):
        pipeline.run(jira_issues(jql=jql, lag=lag, search_page_size=SEARCH_PAGE_SIZE))
        row_counts = pipeline.last_trace.last_normalize_info.row_counts
        assert row_counts.get("issues", 0) == expected, lag
        assert load_table_counts(pipeline, "issues") == {"issues": len(seeded)}


def test_seeded_issues_replace(tmp_path: Path) -> None:
    """Replaces the seeded issues on each run, on filesystem that does not merge"""
    seeded, jql = seeded_issues_with_jql()
    pipeline = filesystem_pipeline("test_jira_seeded_replace", tmp_path.as_uri())
    for _ in range(2):
        issues = jira_issues(jql=jql, write_disposition="replace")
        assert_load_info(pipeline.run(issues, loader_file_format="parquet"))
        # each run reads all issues, not only the ones updated since the last run
        row_counts = pipeline.last_trace.last_normalize_info.row_counts
        assert row_counts["issues"] == len(seeded)
        assert load_table_counts(pipeline, "issues") == {"issues": len(seeded)}
