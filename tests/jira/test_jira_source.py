import os

import dlt
import pytest

from sources.jira import jira, jira_search
from sources.jira.settings import DEFAULT_ENDPOINTS

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
    all_endpoints = jira(page_size=5)
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
    assert set(counts) == {"issues", "projects", "workflows", "workflow_statuses"}
    assert all(count > 0 for count in counts.values())
    # the key each resource declares is unique in its table
    for table, count in counts.items():
        key = DEFAULT_ENDPOINTS[table]["primary_key"]
        assert load_table_distinct_counts(pipeline, key, table) == {table: count}


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

    # load issues from specified Jira queries.
    search = jira_search(page_size=5)
    search.max_table_nesting = 0
    # each query is bounded on its own, so the second one runs and the overlap is real
    search.issues.bind(jql_queries=queries, page_limit=PAGE_LIMIT)
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
    # the queries overlap and the resource deduplicates, so the declared key holds
    counts = load_table_counts(pipeline, "issues")
    assert counts["issues"] > 0
    assert load_table_distinct_counts(pipeline, "id", "issues") == counts


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
