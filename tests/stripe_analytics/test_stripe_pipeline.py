import dlt
import pytest
from pendulum import datetime

from pipelines.stripe_analytics import (
    IncrementalEndpoints,
    UpdatedEndpoints,
    metrics_resource,
    stripe_source,
)

from tests.utils import ALL_DESTINATIONS, assert_load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics_test",
        destination=destination_name,
        dataset_name="stripe_test",
        full_refresh=True,
    )
    data = stripe_source(end_date=datetime(2023, 5, 3))
    # load all endpoints out of the data source
    info = pipeline.run(data)
    assert_load_info(info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_subscription(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics_test",
        destination=destination_name,
        dataset_name="stripe_subscriptions_test",
        full_refresh=True,
    )
    data = stripe_source(end_date=datetime(2023, 5, 3))
    # load the "Subscription" out of the data source
    info = pipeline.run(data.with_resources(UpdatedEndpoints.subscriptions.value))
    # let's print it (pytest -s will show it)
    print(info)
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generates schema. it should contain just one table with user data
    schema = pipeline.default_schema
    user_tables = schema.data_tables()
    assert len(user_tables) == 2
    # tables are typed dicts
    subscription_table = user_tables[0]
    assert subscription_table["name"] == "subscription"
    assert subscription_table["columns"]["created"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["billing_cycle_anchor"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["current_period_end"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["current_period_start"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["plan__created"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["start_date"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["discount__coupon__created"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["discount__start"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["canceled_at"]["data_type"] == "timestamp"
    assert subscription_table["columns"]["ended_at"]["data_type"] == "timestamp"

    # we can also test the data
    with pipeline.sql_client() as c:
        # you can use parametrized queries as well, see python dbapi
        # you can use unqualified table names
        with c.execute_query(
            "SELECT customer FROM subscription WHERE status IN (%s)",
            "canceled",
        ) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 50  # 50 customers canceled their subscriptions


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental_subscriptions_load(destination_name: str) -> None:
    # do the initial load
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics_test",
        destination=destination_name,
        dataset_name="stripe_events_test",
        full_refresh=True,
    )
    data = stripe_source(end_date=datetime(2023, 5, 3))
    info = pipeline.run(data.with_resources(IncrementalEndpoints.events.value))
    assert_load_info(info)

    def get_canceled_subs() -> int:
        with pipeline.sql_client() as c:
            with c.execute_query(
                "SELECT id FROM event WHERE type IN (%s)",
                "customer.subscription.deleted",
            ) as cur:
                rows = list(cur.fetchall())
                return len(rows)  # how many customers canceled their subscriptions

    canceled_subs = get_canceled_subs()
    assert canceled_subs > 0  # should have canceled subscriptions

    # do load with the same range into the existing dataset
    data = stripe_source(end_date=datetime(2023, 5, 3))
    info = pipeline.run(data.with_resources(IncrementalEndpoints.events.value))
    # the dlt figured out that there's no new data at all and skipped the loading package
    assert_load_info(info, expected_load_packages=0)
    # there are no more subscriptions as pipeline is skipping existing subscriptions
    assert get_canceled_subs() == canceled_subs

    # get some new subscriptions
    data = stripe_source()
    info = pipeline.run(data.with_resources(IncrementalEndpoints.events.value))
    # we have new subscriptions in the next day!
    assert_load_info(info)
    assert get_canceled_subs() > canceled_subs


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_metrics(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics_test",
        destination=destination_name,
        dataset_name="stripe_metric_test",
        full_refresh=True,
    )
    data = stripe_source()
    # load the "Subscription" and the "Event" out of the data source
    pipeline.run(
        data.with_resources(
            UpdatedEndpoints.subscriptions.value, IncrementalEndpoints.events.value
        )
    )
    # let's print it (pytest -s will show it)
    resource = metrics_resource()
    load_info = pipeline.run(resource)
    print(load_info)

    assert_load_info(load_info)
