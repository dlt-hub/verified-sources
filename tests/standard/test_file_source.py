from typing import Any, Dict
from urllib.parse import urlparse

import dlt
import pytest

from sources.standard import filesystem_resource
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

from .settings import GLOB_RESULTS, TESTS_BUCKET_URLS


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize("glob_params", GLOB_RESULTS)
def test_file_list(bucket_url: str, glob_params: Dict[str, Any]) -> None:
    @dlt.transformer
    def bypass(items) -> str:
        return items

    # we just pass the glob parameter to the resource if it is not None
    if file_glob := glob_params["glob"]:
        filesystem_res = (
            filesystem_resource(bucket_url=bucket_url, file_glob=file_glob) | bypass
        )
    else:
        filesystem_res = filesystem_resource(bucket_url=bucket_url) | bypass
    # assert filesystem_res.section == "filesystem"
    # filesystem_res.section = "filesystem"
    all_files = list(filesystem_res)
    file_count = len(all_files)
    file_names = [item["file_name"] for item in all_files]
    assert file_count == len(glob_params["file_names"])
    assert file_names == glob_params["file_names"]


@pytest.mark.parametrize("extract_content", [True, False])
@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
def test_load_content_resources(bucket_url: str, extract_content: bool) -> None:
    @dlt.transformer
    def ext_file(items) -> str:
        for item in items:
            if item["file_name"] == "sample.txt":
                content = item.read_bytes()
                assert content == b"dlthub content"
                assert item["size_in_bytes"] == 14
                assert item["file_url"].endswith("/samples/sample.txt")
                assert item["mime_type"] == "text/plain"
        yield items

    all_files = (
        filesystem_resource(
            bucket_url=bucket_url,
            file_glob="sample.txt",
            extract_content=extract_content,
        )
        | ext_file
    )
    # just execute iterator
    files = list(all_files)
    assert len(files) == 1


# @pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
# def test_all_resources(bucket_url: str, destination_name: str) -> None:

#     @dlt.transformer
#     def bypass(items) -> str:
#         return items

#     pipeline = dlt.pipeline(
#         pipeline_name="file_data",
#         destination=destination_name,
#         dataset_name="filesystem_data_duckdb",
#         full_refresh=True,
#     )

#     # Load all files
#     all_files = filesystem_resource(bucket_url=bucket_url, file_glob="csv/*")
#     load_info = pipeline.run(all_files | bypass.with_name("csv_files"))
#     assert_load_info(load_info)

#     table_counts = load_table_counts(pipeline, "csv_files")
#     assert table_counts["csv_files"] == 4


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
def test_incremental_load(bucket_url: str) -> None:
    @dlt.transformer
    def bypass(items) -> str:
        return items

    pipeline = dlt.pipeline(
        pipeline_name="file_data",
        destination="duckdb",
        dataset_name="filesystem_data_duckdb",
        full_refresh=True,
    )

    # Load all files
    all_files = filesystem_resource(bucket_url=bucket_url, file_glob="csv/*")
    # add incremental on modification time
    all_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run(all_files | bypass.with_name("csv_files"))
    assert_load_info(load_info)
    assert pipeline.last_trace.last_normalize_info.row_counts["csv_files"] == 4

    table_counts = load_table_counts(pipeline, "csv_files")
    assert table_counts["csv_files"] == 4

    # load again
    all_files = filesystem_resource(bucket_url=bucket_url, file_glob="csv/*")
    all_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run(all_files | bypass.with_name("csv_files"))
    # nothing into csv_files
    assert "csv_files" not in pipeline.last_trace.last_normalize_info.row_counts
    table_counts = load_table_counts(pipeline, "csv_files")
    assert table_counts["csv_files"] == 4

    # load again into different table
    all_files = filesystem_resource(bucket_url=bucket_url, file_glob="csv/*")
    all_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run(all_files | bypass.with_name("csv_files_2"))
    assert_load_info(load_info)
    assert pipeline.last_trace.last_normalize_info.row_counts["csv_files_2"] == 4
