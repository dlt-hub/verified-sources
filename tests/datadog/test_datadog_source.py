import pytest

import dlt
from dlt.common.utils import uniq_id

from sources.datadog import datadog_source
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

# Needed aenv variables
# SOURCES__DATADOG__SITE=datadoghq.eu
# SOURCES__DATADOG__API_KEY=XXXXXXXXXXXXXXXXXXXXXXX
# SOURCES__DATADOG__APPLICATION_KEY=XXXXXXXXXXXXXXXXXXXX


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def make_pipeline(destination_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="datadog_test",
        destination=destination_name,
        dataset_name="datadog_test_data",
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_resource_authentication(destination_name: str) -> None:
    pipeline = make_pipeline(destination_name)
    data = datadog_source().with_resources("authentication")

    load_info = pipeline.run(
        data,
        table_name="test_authentication",
        write_disposition="replace",
    )
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts.keys() == {"test_authentication"}

    assert table_counts["test_authentication"] == 1
