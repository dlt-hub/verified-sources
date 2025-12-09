from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt
from sources.quickbooks_online import quickbooks_online


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_quickbooks_online(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="quickbooks_customer",
        destination=destination_name,
        dataset_name="duckdb_customer",
        dev_mode=True,
    )
    data = quickbooks_online()
    load_info = pipeline.run(data)
    assert_load_info(load_info)

    expected_tables = ["customer", "invoice"]
    # only those tables in the schema
    assert set(t["name"] for t in pipeline.default_schema.data_tables()) == set(
        expected_tables
    )
    # get counts
    table_counts = load_table_counts(pipeline, *expected_tables)
    # all tables loaded
    assert set(table_counts.keys()) == set(expected_tables)
    assert all(c > 0 for c in table_counts.values())
