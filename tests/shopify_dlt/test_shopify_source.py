import re
from urllib.parse import parse_qs, urlparse

import pytest
from requests_mock import Mocker
import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.sources.helpers import requests

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
from sources.shopify_dlt import shopify_source


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination=destination_name,
        dataset_name="shopify_data",
        full_refresh=True,
    )
    # Set per page limit to ensure we use pagination
    load_info = pipeline.run(shopify_source(items_per_page=5))
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = ["products", "orders", "customers"]
    assert set(table_counts.keys()) > set(expected_tables)
    assert table_counts["products"] == 17
    assert table_counts["orders"] == 13
    assert table_counts["customers"] == 3

    # load again to check there are no dupicates
    load_info = pipeline.run(shopify_source(items_per_page=5))
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert set(table_counts.keys()) > set(expected_tables)
    assert table_counts["products"] == 17
    assert table_counts["orders"] == 13
    assert table_counts["customers"] == 3


def test_start_date() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination="duckdb",
        dataset_name="shopify_data",
        full_refresh=True,
    )

    # we only load objects updated on 05.05. or after
    load_info = pipeline.run(shopify_source(start_date="2023-09-19"))
    assert_load_info(load_info)
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["orders"] == 10


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_end_date_incremental(destination_name: str) -> None:
    """Load chunk with start/end date and incrementally with start date after"""
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination="duckdb",
        dataset_name="shopify_data",
        full_refresh=True,
    )

    # Set start date to the exact timestamp of the first order in the test account
    start_date = pendulum.DateTime(2023, 7, 21, 15, 31, 30)
    # End date is exact timestamp of an order in the middle
    end_date = pendulum.DateTime(2023, 9, 19, 20, 59, 50).in_tz("UTC")

    # Load orders between start/end date
    # Use two different date formats and timezones to ensure they're handled correctly
    data = shopify_source(
        start_date=start_date,
        end_date=end_date.in_timezone("EST").isoformat(),
        items_per_page=5,
    ).with_resources("orders")

    info = pipeline.run(data, write_disposition="append")
    assert_load_info(info)

    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["orders"] == 2

    with pipeline.sql_client() as client:
        rows = [
            (row[0], pendulum.instance(row[1]))
            for row in client.execute_sql(
                "SELECT id, updated_at FROM orders ORDER BY updated_at"
            )
        ]

    start_date_utc = start_date.in_tz("UTC")
    dest_dates = [row[1] for row in rows]

    # Loaded range falls correctly between start/end date
    assert min(dest_dates) == start_date_utc
    assert start_date_utc < max(dest_dates) < end_date

    # Load again with incremental, starting at end_date
    data = shopify_source(start_date=end_date, items_per_page=5).with_resources(
        "orders"
    )

    info = pipeline.run(data, write_disposition="append")
    assert_load_info(info)

    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["orders"] == 10

    with pipeline.sql_client() as client:
        rows2 = [
            (row[0], pendulum.instance(row[1]))
            for row in client.execute_sql(
                "SELECT id, updated_at FROM orders ORDER BY updated_at"
            )
        ]
    dest_dates2 = [row[1] for row in rows2]
    assert len(rows2) > len(rows)
    assert end_date in dest_dates2

    # No duplicates
    assert len(rows2) == len(set(rows2))


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_order_status(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination=destination_name,
        dataset_name="shopify_data",
        full_refresh=True,
    )

    data = shopify_source(
        order_status="closed",
    ).with_resources("orders")

    info = pipeline.run(data)
    assert_load_info(info)

    # Check that all loaded orders are closed
    with pipeline.sql_client() as client:
        rows = [row[0] for row in client.execute_sql("SELECT closed_at FROM orders")]

    assert all(rows)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_min_created_at(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination=destination_name,
        dataset_name="shopify_data",
        full_refresh=True,
    )

    # Order create before created_at_min, but updated after
    # our start date should not be included. E.g.
    # 3 | 2023-05-04 13:39:36+00 | 2023-07-24 18:20:09+00

    # Set the min to 1s after the test order was created
    created_at_min = ensure_pendulum_datetime("2023-05-04T13:39:37Z")

    data = shopify_source(
        start_date="2020-01-01",
        created_at_min=created_at_min,
    ).with_resources("orders")

    info = pipeline.run(data)

    assert_load_info(info)

    with pipeline.sql_client() as client:
        rows = [
            (ensure_pendulum_datetime(row[0]), ensure_pendulum_datetime(row[1]))
            for row in client.execute_sql(
                "SELECT updated_at, created_at FROM orders ORDER BY updated_at"
            )
        ]
    update_dates, create_dates = list(zip(*rows))

    # All loaded create/update dates are higher than created_at_min
    assert min(create_dates) >= created_at_min
    assert min(update_dates) >= created_at_min


@pytest.mark.parametrize("resource_name", ["orders", "customers", "products"])
def test_request_params(resource_name: str) -> None:
    """Test source arguments are passed to the request query params"""
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        dataset_name="shopify_data",
        full_refresh=True,
    )

    data = shopify_source(
        order_status="closed",
        start_date="2023-05-05",
        end_date="2023-05-06",
        items_per_page=100,
        created_at_min="2020-01-01",
    ).with_resources(resource_name)

    with Mocker(session=requests.client.session) as m:
        m.get(
            re.compile(r"/{}.json".format(resource_name)),
            json={resource_name: []},
        )

        pipeline.extract(data)

    # verify the last request query params
    params = parse_qs(urlparse(m.last_request.url).query)

    assert ensure_pendulum_datetime(
        params["updated_at_min"][0]
    ) == ensure_pendulum_datetime("2023-05-05")
    assert ensure_pendulum_datetime(
        params["updated_at_max"][0]
    ) == ensure_pendulum_datetime("2023-05-06")
    assert ensure_pendulum_datetime(
        params["created_at_min"][0]
    ) == ensure_pendulum_datetime("2020-01-01")
    assert params["limit"] == ["100"]
    if resource_name == "orders":
        assert params["status"] == ["closed"]
