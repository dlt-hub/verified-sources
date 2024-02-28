"""
This module contains tests for validating the data loading functionality of
the Freshdesk integration.
"""

import dlt
import pytest
from freshdesk import freshdesk_source

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_endpoints(destination_name: str) -> None:
    """
    Tests loading data from all Freshdesk default endpoints to a specified
    destination.

    This test creates a DLT pipeline configured for a full refresh load, runs
    the pipeline to load data from all Freshdesk endpoints, and then verifies
    that the load was successful and the data counts in the destination match
    expected values. It also tests the pipeline's ability to handle incremental
    loads correctly by re-running the pipeline and ensuring data counts remain
    unchanged.

    Parameters:
        destination_name (str): The name of the destination where data will
                                be loaded.
    """
    # Set up and run the DLT pipeline for a full refresh load.
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

    # Verify data counts in the destination match expected values.
    expected_counts = {
        "agents": 1,
        "companies": 1,
        "contacts": 16,
        "groups": 3,
        "roles": 8,
        "tickets": 3,
    }
    assert (
        load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        == expected_counts
    )

    # Test incremental loading by re-running the pipeline and verifying data
    # counts remain unchanged.
    data = freshdesk_source()
    info = pipeline.run(data)
    assert_load_info(info, expected_load_packages=0)
    assert (
        load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        == expected_counts
    )
