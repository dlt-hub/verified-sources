import dlt
import pytest

from sources.jira import jira, jira_search

from tests.utils import ALL_DESTINATIONS, assert_load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_endpoints(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        full_refresh=True,
    )
    info = pipeline.run(jira())
    # lets print it (pytest -s will show it)
    print(info)
    # make sure all jobs were loaded
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_users(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        full_refresh=True,
    )
    data = jira()
    # load the "users" out of the data source
    info = pipeline.run(data.with_resources("users"))
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

    assert users_table["name"] == "users"

    # we can also test the data
    with pipeline.sql_client() as c:
        # you can use parametrized queries as well, see python dbapi
        # you can use unqualified table names
        with c.execute_query(
            "SELECT display_name, account_type FROM users WHERE display_name IN (%s, %s) ORDER BY display_name ASC",
            "Trello",
            "Adrian Brudaru",
        ) as cur:
            rows = list(cur.fetchall())

            assert len(rows) == 3
            assert rows[0][0] == "Adrian Brudaru"
            assert rows[0][1] == "atlassian"  # Adrian has 'atlassian' account type


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_query_issues(destination_name: str) -> None:
    # test queries
    queries = [
        "created >= -30d order by created DESC",
        'created >= -30d AND assignee in (619652abc510bc006b40d007) AND project = DEV AND issuetype = Epic AND status = "In Progress" order by created DESC',
    ]

    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        full_refresh=True,
    )

    # load issues from specified Jira queries.
    info = pipeline.run(jira_search().issues(jql_queries=queries))
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

    # we can also test the data
    with pipeline.sql_client() as c:
        # you can use parametrized queries as well, see python dbapi
        # you can use unqualified table names
        with c.execute_query("SELECT * FROM issues") as cur:
            rows = list(cur.fetchall())

            assert len(rows) == 2
            assert rows[0][1] == 50
