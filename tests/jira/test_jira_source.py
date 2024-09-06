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
    assert load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    ) == {"issues": 10, "projects": 1, "workflows": 3}


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_users(destination_name: str) -> None:
    # mind the dev_mode flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        dev_mode=True,
    )
    data = jira()
    all_endpoints = jira(page_size=5)
    all_endpoints.max_table_nesting = 0
    # load the "users" out of the data source
    info = pipeline.run(data.with_resources("users"))
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generates schema. it should contain just one table with data
    schema = pipeline.default_schema
    data_tables = schema.data_tables()
    assert len(data_tables) == 1
    # tables are typed dicts
    users_table = data_tables[0]
    assert users_table["name"] == "users"
    # print(pipeline.default_schema.to_pretty_yaml())
    # check table counts
    assert (
        load_table_counts(pipeline, "users")
        == load_table_distinct_counts(pipeline, "account_id", "users")
        == {"users": 19}
    )

    # we can also test the data
    with pipeline.sql_client() as c:
        # you can use parametrized queries as well, see python dbapi
        # you can use unqualified table names
        with c.execute_query(
            "SELECT display_name, account_type FROM users WHERE display_name IN (%s, %s) ORDER BY display_name ASC",
            "Trello",
            "Anna Hofffmann",
        ) as cur:
            rows = list(cur.fetchall())

            assert len(rows) == 2
            assert rows[0][0] == "Trello"
            assert rows[0][1] == "app"  # Trello has 'app' account type


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_query_issues(destination_name: str) -> None:
    # test queries
    queries = [
        "created >= -30d order by created DESC",  # nothing returned
        'key = "KAN-2"',  # one returned
        "",  # all returned
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
    assert load_table_counts(pipeline, "issues") == {
        "issues": 11
    }  # values are changing (decrease)
    # distinct. actually, we have only 10 unique issues in the project
    assert load_table_distinct_counts(pipeline, "id", "issues") == {"issues": 10}
