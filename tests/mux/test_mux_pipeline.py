import pytest

from pipelines.mux import mux_source

import dlt

from tests.utils import ALL_DESTINATIONS, assert_load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_selected_notion_database(destination_name: str):
    pipeline = dlt.pipeline(
        pipeline_name="mux",
        destination=destination_name,
        dataset_name="mux_data",
        full_refresh=True,
    )

    info = pipeline.run(mux_source())
    assert_load_info(info)
