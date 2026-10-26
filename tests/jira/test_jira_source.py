import dlt
import pytest

from sources.jira import jira, jira_search

from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    load_table_distinct_counts,
)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_endpoints(destination_name: str) -> None:
    # mind the dev_mode flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    all_endpoints = jira(page_size=1)
    # disable user - use next test for that
    all_endpoints.users.selected = False
    # do not load child tables
    all_endpoints.max_table_nesting = 0
    info = pipeline.run(all_endpoints)
    # make sure all jobs were loaded
    assert_load_info(info)
    counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert set(counts) == {"issues", "projects", "workflows"}
    assert all(count > 0 for count in counts.values())


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_users(destination_name: str) -> None:
    # mind the dev_mode flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    source = jira(page_size=5)
    source.max_table_nesting = 0
    # load the "users" out of the data source
    info = pipeline.run(source.with_resources("users"))
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generates schema. it should contain just one table with data
    schema = pipeline.default_schema
    data_tables = schema.data_tables()
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
        "created >= -1d order by created DESC",  # nothing returned
        'key = "MDP-10"',  # one returned
        "created >= '2000-01-01' order by created DESC",  # all returned
    ]

    # mind the `dev_mode` flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )

    # load issues from specified Jira queries.
    search = jira_search()
    search.max_table_nesting = 0
    search.issues.bind(jql_queries=queries)
    info = pipeline.run(search)
    # lets print it (pytest -s will show it)
    print(info)
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generates schema. it should contain just one table with data
    schema = pipeline.default_schema
    data_tables = schema.data_tables()
    assert len(data_tables) == 1
    # tables are typed dicts
    users_table = data_tables[0]
    assert users_table["name"] == "issues"
    # overlapping queries write every match under replace, so only the distinct count is stable
    distinct = load_table_distinct_counts(pipeline, "id", "issues")
    assert distinct["issues"] > 0
    assert load_table_counts(pipeline, "issues")["issues"] >= distinct["issues"]
