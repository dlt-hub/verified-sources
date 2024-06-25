import dlt
import pytest
from pendulum import datetime

from sources.stripe_analytics import (
    incremental_stripe_source,
    stripe_source,
)

from tests.utils import ALL_DESTINATIONS, assert_load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental_resources(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset.
    # this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics_test_inc",
        destination=destination_name,
        dataset_name="stripe_incremental_test",
        full_refresh=True,
    )
    data = incremental_stripe_source()
    # load all endpoints out of the data source
    info = pipeline.run(data)
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_updated_resources(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset.
    # this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics_test_upd",
        destination=destination_name,
        dataset_name="stripe_updated_test",
        full_refresh=True,
    )
    data = stripe_source()
    # load all endpoints out of the data source
    info = pipeline.run(data)
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_subscription(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset.
    # this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics_subscriptions_test",
        destination=destination_name,
        dataset_name="stripe_subscriptions_test",
        full_refresh=True,
    )
    data = stripe_source(endpoints=("Subscription",), end_date=datetime(2024, 5, 3))
    data.resources["Subscription"].apply_hints(
        columns={
            "created": {"data_type": "timestamp"},
        }
    )
    # load the "Subscription" out of the data source
    info = pipeline.run(data)
    # let's print it (pytest -s will show it)
    print(info)
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generated schema.
    # it should contain just three tables: subscription, subscription__items__data and subscription__discounts
    schema = pipeline.default_schema
    user_tables = schema.data_tables()
    assert len(user_tables) == 3
    # tables are typed dicts
    subscription_table = user_tables[0]
    assert subscription_table["name"] == "subscription"
    assert subscription_table["columns"]["created"]["data_type"] == "timestamp"

    # we can also test the data
    with pipeline.sql_client() as c:
        # you can use parametrized queries as well, see python dbapi
        # you can use unqualified table names
        with c.execute_query(
            "SELECT customer FROM subscription WHERE status IN (%s)",
            "active",
        ) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3  # 3 customers have active subscriptions


@pytest.mark.skip(
    "Stripe events expire after 30 days, generate events to run this test"
)
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental_event_load(destination_name: str) -> None:
    # do the initial load
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics_event_test",
        destination=destination_name,
        dataset_name="stripe_event_test",
        full_refresh=True,
    )
    data = incremental_stripe_source(
        endpoints=("Event",), end_date=datetime(2024, 2, 1)
    )
    info = pipeline.run(data)
    assert_load_info(info)

    def get_active_subs() -> int:
        with pipeline.sql_client() as c:
            with c.execute_query("SELECT id FROM event") as cur:
                rows = list(cur.fetchall())
                return len(rows)

    #
    active_subs = get_active_subs()
    assert active_subs > 0  # should have active subscriptions

    # do load with the same range into the existing dataset
    data = incremental_stripe_source(
        endpoints=("Event",), end_date=datetime(2024, 2, 1)
    )
    info = pipeline.run(data)
    # the dlt figured out that there's no new data at all and skipped the loading package
    assert_load_info(info, expected_load_packages=0)
    # there are no more subscriptions as a pipeline is skipping existing subscriptions
    assert get_active_subs() == active_subs

    # get some new subscriptions
    data = incremental_stripe_source(endpoints=("Event",))
    info = pipeline.run(data)
    # we have new subscriptions in the next day!
    assert_load_info(info)
    assert get_active_subs() > active_subs
