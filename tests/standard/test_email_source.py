import dlt
import pytest

from sources.standard.inbox import inbox_source
from tests.utils import ALL_DESTINATIONS, assert_load_info


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
        for item in items:
            # Make sure just filtered emails are processed
            assert "josue@sehnem.com" in item["From"]
            if item["file_name"] == "dlthub.txt":
                # Find the attachment with the file name and assert the loaded content
                content = item.read_bytes()
                assert item["file_name"] == "dlthub.txt"
        assert content == b"dlthub content"

    data_source = inbox_source(
        filter_by_emails=("josue@sehnem.com",),
        attachments=True,
        chunksize=10,
        filter_by_mime_type=("text/txt",),
    )

    attachments = data_source.resources["attachments"] | ext_file

    load_info = pipeline.run(attachments)
    assert_load_info(load_info)
