import dlt
import pytest

from sources.standard.filesystem import filesystem_resource
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="file_source",
        destination=destination_name,
        dataset_name="file_source_data",
        full_refresh=True,
    )

    # Load all files
    all_files = filesystem_resource()
    load_info = pipeline.run(all_files)
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline, "filesystem")
    assert table_counts["filesystem"] == 5


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_filtered_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="file_source",
        destination=destination_name,
        dataset_name="file_source_data",
        full_refresh=True,
    )

    # Load filter files
    all_files = filesystem_resource(filename_filter="mlb*.csv")
    load_info = pipeline.run(all_files)
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline, "filesystem")
    assert table_counts["filesystem"] == 2


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_content_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="file_source",
        destination=destination_name,
        dataset_name="file_source_data",
        full_refresh=True,
    )

    @dlt.transformer
    def ext_file(items) -> str:
        content = items[0].read()
        assert content == b"dlthub content"

    # Load filter files
    all_files = (
        filesystem_resource(filename_filter="sample.txt", extract_content=True)
        | ext_file
    )
    load_info = pipeline.run(all_files)
    assert_load_info(load_info)
