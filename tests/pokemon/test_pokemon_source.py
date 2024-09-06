from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt
from sources.pokemon import source


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="template",
        destination=destination_name,
        dataset_name="template_data",
        dev_mode=True,
    )
    load_info = pipeline.run(source())
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = ["pokemon", "berries"]
    assert set(table_counts.keys()) >= set(expected_tables)

    assert table_counts["pokemon"] == 20
    assert table_counts["berries"] == 20

    # load tasks incrementally
    load_info = pipeline.run(source())
    assert table_counts == load_table_counts(pipeline, *table_names)
