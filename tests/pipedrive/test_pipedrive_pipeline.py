import pytest

import dlt

from pipelines.pipedrive import pipedrive_source

from tests.utils import ALL_DESTINATIONS, assert_load_info


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination=destination_name, dataset_name='pipedrive_data', full_refresh=True)
    load_info = pipeline.run(pipedrive_source())
    print(load_info)
    assert_load_info(load_info)
    # TODO: validate schema and data: write a test helper for that
