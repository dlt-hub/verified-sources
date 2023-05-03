from tests.utils import ALL_DESTINATIONS, assert_load_info
import pytest
import dlt
from pipelines.asana_dlt import asana_source



@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(pipeline_name='hubspot', destination=destination_name, dataset_name='asana_data', full_refresh=True)
    load_info = pipeline.run(asana_source())
    print(load_info)
    assert_load_info(load_info)
