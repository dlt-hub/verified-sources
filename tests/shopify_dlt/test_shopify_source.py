from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt
from sources.shopify_dlt import shopify_source


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination=destination_name,
        dataset_name="shopify_data",
        full_refresh=True,
    )
    load_info = pipeline.run(shopify_source())
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = ["products", "orders", "customers"]
    assert set(table_counts.keys()) > set(expected_tables)
    assert table_counts["products"] == 17
    assert table_counts["orders"] == 11
    assert table_counts["customers"] == 3

    # load again to check there are no dupicates
    load_info = pipeline.run(shopify_source())
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert set(table_counts.keys()) > set(expected_tables)
    assert table_counts["products"] == 17
    assert table_counts["orders"] == 11
    assert table_counts["customers"] == 3


def test_start_date() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination="duckdb",
        dataset_name="shopify_data",
        full_refresh=True,
    )

    # we only load objects created on 05.05. or after which is only one at this point
    load_info = pipeline.run(shopify_source(start_date="2023-05-05"))
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert table_counts["orders"] == 1
