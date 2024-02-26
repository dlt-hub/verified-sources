import dlt
import pytest

from sources.freshdesk import freshdesk_source
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_endpoints(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        full_refresh=True,
    )
    all_endpoints = freshdesk_source()
    all_endpoints.max_table_nesting = 0
    info = pipeline.run(all_endpoints)
    assert_load_info(info)

    # Expected counts for each table
    expected_counts = {
        "agents": 1,
        "companies": 1,
        "contacts": 16,
        "groups": 3,
        "roles": 8,
        "tickets": 3,
    }

    # Function to get count from the database for a given table
    assert (
        load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        == expected_counts
    )

    # Run pipeline again to test incremental loading
    data = freshdesk_source()
    info = pipeline.run(data)
    assert_load_info(info, expected_load_packages=0)

    # Assert counts remain unchanged after re-running pipeline
    assert (
        load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        == expected_counts
    )
