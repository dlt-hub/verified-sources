import pytest

import dlt

from pipelines.workable import workable_source

from tests.utils import ALL_DESTINATIONS, assert_load_info


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_all_data(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_test",
    )
    load_data = workable_source()
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)
    # make sure all jobs were loaded
    assert_load_info(load_info)


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_candidates(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_test",
    )
    load_data = workable_source().with_resources("candidates")
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)
    # make sure all jobs were loaded
    assert_load_info(load_info)
    # now let's inspect the generated schema. it should contain just one table with user data
    schema = pipeline.default_schema
    user_tables = schema.data_tables()
    assert len(user_tables) == 1
    # tables are typed dicts
    subscription_table = user_tables[0]
    assert subscription_table["name"] == "candidates"
    assert subscription_table["columns"]["created_at"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["updated_at"]["data_type"] == "timestamp"

    # we can also test the data
    with pipeline.sql_client() as c:
        # you can use parametrized queries as well, see python dbapi
        # you can use unqualified table names
        with c.execute_query(
            "SELECT * FROM candidates WHERE stage IN (%s, %s)",
            "Hired", "Offer"
        ) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 4  # 4 candidates passed all stages of the interview


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental_event_load(destination_name: str) -> None:
    # do the initial load
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_test",
    )
    load_data = workable_source().with_resources("candidates")
    load_info = pipeline.run(load_data)
    assert_load_info(load_info)

    def get_succeeded_candidates() -> int:
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query(
                "SELECT * FROM candidates WHERE stage IN (%s, %s)",
                "Hired", "Offer"
            ) as cur:
                rows = list(cur.fetchall())
                return len(rows) # 4 candidates passed all stages of the interview

    succeeded_candidates = get_succeeded_candidates()
    assert succeeded_candidates > 0  # should have all succeeded candidates

    # do load with the same range into the existing dataset
    load_data = workable_source().with_resources("candidates")
    load_info = pipeline.run(load_data)
    # the dlt figured out that there's no new data at all and skipped the loading package
    assert_load_info(load_info, expected_load_packages=0)
    # there are no more subscriptions as a pipeline is skipping existing subscriptions
    assert get_succeeded_candidates() == succeeded_candidates


