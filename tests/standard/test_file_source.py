from typing import Any, Dict
from urllib.parse import urlparse

import dlt
import pytest

from sources.standard.filesystem import filesystem_resource
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

from .settings import GLOB_RESULTS, TESTS_BUCKET_URLS


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize("glob_params", GLOB_RESULTS)
def test_file_list(bucket_url: str, glob_params: Dict[str, Any]) -> None:
    @dlt.transformer
    def assert_files(items) -> str:
        file_count = len(items)
        file_names = [item["file_name"] for item in items]
        assert file_count == len(glob_params["file_names"])
        assert file_names == glob_params["file_names"]

    # we just pass the glob parameter to the resource if it is not None
    if file_glob := glob_params["glob"]:
        list(
            filesystem_resource(bucket_url=bucket_url, file_glob=file_glob)
            | assert_files
        )
    else:
        list(filesystem_resource(bucket_url=bucket_url) | assert_files)


@pytest.mark.parametrize("extract_content", [True, False])
@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_content_resources(
    bucket_url: str, destination_name: str, extract_content: bool
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="file_source",
        destination=destination_name,
        dataset_name="file_source_data",
        full_refresh=True,
    )

    @dlt.transformer
    def ext_file(items) -> str:
        for item in items:
            if item["file_name"] == "sample.txt":
                content = item.read_bytes()
                assert content == b"dlthub content"
                assert item["size_in_bytes"] == 14
                assert item["file_url"].endswith("/samples/sample.txt")
                assert item["mime_type"] == "text/plain"

    all_files = (
        filesystem_resource(
            bucket_url=bucket_url,
            file_glob="sample.txt",
            extract_content=extract_content,
        )
        | ext_file
    )
    load_info = pipeline.run(all_files)
    assert_load_info(load_info)


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(bucket_url: str, destination_name: str) -> None:
    bucket_url_parsed = urlparse(bucket_url)
    protocol = bucket_url_parsed.scheme or "file"
    pipeline = dlt.pipeline(
        pipeline_name="file_data",
        destination=destination_name,
        dataset_name=f"filesystem_data_{protocol}",
        full_refresh=True,
    )

    # Load all files
    all_files = filesystem_resource(bucket_url=bucket_url, file_glob="csv/*")
    all_files.table_name = "filesystem"

    load_info = pipeline.run(all_files)
    assert_load_info(load_info)

    table_counts = load_table_counts(pipeline, "filesystem")
    assert table_counts["filesystem"] == 4
