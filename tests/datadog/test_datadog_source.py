import pytest

import dlt
from dlt.common.utils import uniq_id

from sources.datadog import datadog_source


from tests.utils import ALL_DESTINATIONS, assert_load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def make_pipeline(destination_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="datadog_test",
        destination=destination_name,
        dataset_name="datadog_test_data" + uniq_id(),
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_resource_authentication(destination_name: str) -> None:
    pipeline = make_pipeline(destination_name)
    data = datadog_source(table_name="test_authentication").with_resources(
        "authentication"
    )

    info = pipeline.run(data, write_disposition="replace")
    assert_load_info(info)

    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["authentication"] == 1
