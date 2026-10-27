import os
from typing import Any, Dict, List

import dlt
import pytest
from dlt.sources.helpers.requests import HTTPError

from sources.jira import issue_comments, jira, jira_issues, jira_search
from sources.jira.settings import DEFAULT_ENDPOINTS

from tests.jira.seed_test_data import SEED_LABEL, Jira
from tests.jira.utils import CREDENTIALS
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
    # lets print it (pytest -s will show it)
    print(info)
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
    site = Jira(
        dlt.secrets["sources.jira.subdomain"],
        dlt.secrets["sources.jira.email"],
        dlt.secrets["sources.jira.api_token"],
        "TST",
    )
    issue_ids = site.search_ids(f'labels = "{SEED_LABEL}-comment-changes"')
    if not issue_ids:
        pytest.skip(
            "seed the test site: seed_test_data.py --apply --cases comment_changes"
        )
    path = f"issue/{issue_ids[0]}/comment"
    created: List[str] = [
        site.request("POST", path, json={"body": f"online test {n}"})["id"]
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
        with pipeline.sql_client() as client:
            rows = client.execute_sql("SELECT id, body FROM issue_comments")
        return {comment_id: body for comment_id, body in rows if comment_id in created}

    try:
        assert load() == {created[0]: "online test 1", created[1]: "online test 2"}
        site.request("PUT", f"{path}/{created[0]}", json={"body": "edited"})
        site.request("DELETE", f"{path}/{created[1]}")
        # the issue was updated within lag and is read again
        assert load() == {created[0]: "edited"}
        site.request("DELETE", f"{path}/{created[0]}")
        assert load() == {}
    finally:
        for comment_id in created:
            try:
                site.request("DELETE", f"{path}/{comment_id}")
            except HTTPError:
                pass


# the structured types are checked in Snowflake
@pytest.mark.parametrize(
    "destination_name", [name for name in ALL_DESTINATIONS if name == "snowflake"]
)
def test_issues_nested_types_snowflake(
    destination_name: str, captured_mock: Dict[str, Any]
) -> None:
    os.environ["DESTINATION__SNOWFLAKE__USE_NESTED_TYPES"] = "true"
    pipeline = dlt.pipeline(
        "jira_nested_snowflake", destination=destination_name, dev_mode=True
    )
    for _ in range(2):
        # the second run merges issues updated within lag into the structured columns
        issues = jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True)
        assert_load_info(pipeline.run(issues, loader_file_format="parquet"))

    text, number = "VARCHAR(16777216)", "NUMBER(19,0)"
    issue_ref = f"OBJECT(id {text}, key {text})"
    with pipeline.sql_client() as client:
        table = client.make_qualified_table_name("issues")
        types = {
            row[0]: row[1] for row in client.execute_sql(f"DESCRIBE TABLE {table}")
        }
        rows = client.execute_sql(
            f"""
            SELECT
                key,
                customfield_10020[0]:"name"::string,
                customfield_10020[0]:"boardId"::int,
                customfield_10046:"child":"value"::string,
                attachment[0]:"author":"accountId"::string,
                issuelinks[0]:"outwardIssue":"key"::string,
                subtasks[1]:"key"::string,
                parent:"key"::string,
                status:"statusCategory":"key"::string,
                ARRAY_SIZE(labels),
                customfield_10019
            FROM {table}
            """
        )
    # nested types are created as structured types with Jira keys
    assert types["CUSTOMFIELD_10020"] == (
        f"ARRAY(OBJECT(id {number}, name {text}, state {text}, boardId {number}, goal {text},"
        f" startDate {text}, endDate {text}, completeDate {text}))"
    )
    assert types["ISSUELINKS"] == (
        f"ARRAY(OBJECT(id {text}, type OBJECT(id {text}, name {text}, inward {text}, outward"
        f" {text}), inwardIssue {issue_ref}, outwardIssue {issue_ref}))"
    )
    assert types["ATTACHMENT"] == (
        f"ARRAY(OBJECT(id {text}, filename {text}, mimeType {text}, size {number}, created"
        f" {text}, author OBJECT(accountId {text}, displayName {text})))"
    )
    assert types["CUSTOMFIELD_10046"] == (
        f"OBJECT(id {text}, value {text}, child OBJECT(id {text}, value {text}))"
    )
    assert types["STATUS"] == (
        f"OBJECT(id {text}, name {text}, statusCategory OBJECT(id {number}, key {text}, name"
        f" {text}))"
    )
    assert types["PARENT"] == issue_ref
    assert types["LABELS"] == f"ARRAY({text})"
    assert types["CUSTOMFIELD_10019"] == text

    values = {row[0]: row[1:] for row in rows}
    assert len(values) == 8
    assert values["TST-13"][:2] == ("TST Sprint 3", 2)
    assert values["TST-24"][2] == "[dummy] rest_api"
    assert values["TST-17"][3] == "account-1"
    assert values["TST-18"][4:6] == ("TST-21", "TST-20")
    assert values["TST-19"][6] == "TST-18"
    labels = next(i for i in captured_mock["issues"] if i["key"] == "TST-24")["fields"][
        "labels"
    ]
    assert values["TST-24"][7:] == ("new", len(labels), "0|i0008n:")
