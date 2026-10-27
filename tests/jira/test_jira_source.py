import os
from collections import Counter
from datetime import timedelta
from typing import Dict, List

import dlt
import pytest
from dlt.common.time import ensure_datetime_in_tz
from dlt.sources.helpers.requests import HTTPError

from sources.jira import (
    issue_changelogs,
    issue_comments,
    jira,
    jira_issues,
    jira_search,
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
from tests.jira.utils import SEED_LABEL
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    load_table_distinct_counts,
    load_tables_to_dicts,
)

# pages per resource, so the run stays bounded on a site of any size
PAGE_LIMIT = 2


@pytest.fixture(autouse=True)
def jira_credentials() -> None:
    """These tests load from a live Jira site. The unit tests cover the source without one."""
    subdomain = dlt.secrets.get("sources.jira.subdomain")
    if subdomain and subdomain != "set me up":
        return
    if os.environ.get("CI"):
        pytest.fail(
            "[sources.jira] is missing from DLT_SECRETS_TOML, so CI tested nothing"
        )
    pytest.skip("set [sources.jira] in sources/.dlt/secrets.toml to run these")


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_endpoints(destination_name: str) -> None:
    # mind the dev_mode flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    # seeded issues with comments, changes and worklogs. `add_limit` does not bound issues,
    # they are read by a transformer
    all_endpoints = jira(
        page_size=5,
        jql=f'labels = "{SEED_LABEL}" AND NOT labels = "{SEED_LABEL}-bulk-issues"',
    )
    # disable user - use next test for that
    all_endpoints.users.selected = False
    # do not load child tables
    all_endpoints.max_table_nesting = 0
    all_endpoints.add_limit(PAGE_LIMIT)
    info = pipeline.run(all_endpoints)
    # make sure all jobs were loaded
    assert_load_info(info)
    counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    keys = {
        "issues": "id",
        "issue_comments": "id",
        "issue_changelogs": "id",
        "worklogs": "id",
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
    # mind the `dev_mode` flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    source = jira(page_size=5)
    source.max_table_nesting = 0
    # load the "users" out of the data source
    source = source.with_resources("users")
    source.add_limit(PAGE_LIMIT)
    info = pipeline.run(source)
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generates schema. it should contain just one table with data
    data_tables = pipeline.default_schema.data_tables()
    assert len(data_tables) == 1
    # tables are typed dicts
    users_table = data_tables[0]
    assert users_table["name"] == "users"
    counts = load_table_counts(pipeline, "users")
    assert counts == load_table_distinct_counts(pipeline, "account_id", "users")
    assert counts["users"] > 0


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_query_issues(destination_name: str) -> None:
    # test queries
    queries = [
        "created >= -1d order by created DESC",  # issues from the last day
        "created >= '2000-01-01' order by created DESC",  # every issue, overlaps the first
    ]

    # mind the `dev_mode` flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_jira_search",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )

    # load issues from specified Jira queries, each query is bounded on its own, so the second
    # one runs and the overlap is real
    search = jira_search(page_size=5, jql_queries=queries, page_limit=PAGE_LIMIT)
    search.max_table_nesting = 0
    info = pipeline.run(search)
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generated schema. it should contain just one table with data
    data_tables = pipeline.default_schema.data_tables()
    assert len(data_tables) == 1
    # tables are typed dicts
    users_table = data_tables[0]
    assert users_table["name"] == "issues"
    # an issue found by both queries is loaded for each, the key is the issue and the query
    rows = load_tables_to_dicts(pipeline, "issues")["issues"]
    assert {row["query_index"] for row in rows} == {0, 1}
    keys = [(row["id"], row["query_index"]) for row in rows]
    assert len(keys) == len(set(keys))


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
def test_seeded_issues_full_and_incremental(destination_name: str) -> None:
    """Loads all seeded issues with their comments, changelogs and worklogs, then in backfill
    windows and incrementally. The counts and values are predicted by the seed cases and the
    manifest of seeded issues, nothing is derived from the loaded data.
    """
    # the comments test writes to the comment changes issue
    excluded = ("comment_changes",)
    seeded = seeded_issues(excluded)
    assert len(seeded) == expected_issues(excluded)
    jql = f'labels = "{SEED_LABEL}" AND NOT labels = "{case_label(*excluded)}"'

    def case_id(case: str) -> str:
        (issue,) = issues_of_case(seeded, case)
        return str(issue["id"])

    pipeline = dlt.pipeline(
        pipeline_name="test_jira_seeded",
        destination=destination_name,
        dataset_name="test_jira_seeded",
        dev_mode=True,
    )
    issues = jira_issues(jql=jql)
    info = pipeline.run(
        [
            issues,
            issues | issue_comments,
            issues | issue_changelogs,
            worklogs(),
        ]
    )
    assert_load_info(info)
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

    # backfill windows over `created`, each with the number of issues the manifest predicts
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
        predicted = sum(start <= value < end for value in created)
        assert predicted > 0
        window = dlt.sources.incremental("created", initial_value=start, end_value=end)
        assert_load_info(backfill.run(jira_issues(jql=jql, incremental=window)))
        assert backfill.last_trace.last_normalize_info.row_counts["issues"] == predicted
    assert load_table_counts(backfill, "issues") == {"issues": len(seeded)}

    # runs without changes: the issues updated within lag of the last one are read again, then
    # without lag the issues updated last are read once more and recorded, then nothing
    updated = [ensure_datetime_in_tz(issue["updated"]) for issue in seeded]
    last = max(updated)
    within_lag = sum(
        value >= last - timedelta(seconds=DEFAULT_LAG) for value in updated
    )
    at_last = sum(value == last for value in updated)
    for lag, predicted in ((None, within_lag), (0, at_last), (0, 0)):
        pipeline.run(jira_issues(jql=jql, lag=lag))
        row_counts = pipeline.last_trace.last_normalize_info.row_counts
        assert row_counts.get("issues", 0) == predicted, lag
        assert load_table_counts(pipeline, "issues") == {"issues": len(seeded)}
