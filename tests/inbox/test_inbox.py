import pytest
import imaplib

import dlt
from dlt.common import pendulum
from dlt.extract.exceptions import ResourceExtractionError

from sources.inbox import inbox_source

from tests.utils import assert_query_data


def test_load_uids_incremental() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_load_uids_incremental", destination="dummy"
    )
    source = inbox_source()
    pipeline.extract(source.uids)
    norm_info = pipeline.normalize()
    # we have some messages
    assert norm_info.row_counts["uids"] > 4

    # extract again, this time all should be skipped due to incremental
    pipeline.extract(inbox_source().uids)
    norm_info = pipeline.normalize()
    # we have some messages
    assert "uids" not in norm_info.row_counts

    # backtrack by one message
    # print(pipeline.state)


def test_load_uids_options() -> None:
    # filter by from
    assert (
        len(
            list(
                inbox_source(
                    filter_emails=("josue@sehnem.com", "josue@sehnem.com")
                ).uids
            )
        )
        > 0
    )
    assert len(list(inbox_source(filter_emails=("unknown@dlthub.com")).uids)) == 0
    # change folder
    with pytest.raises(ResourceExtractionError) as ex_err:
        list(inbox_source(folder="Sent").uids)
    assert isinstance(ex_err.value.__context__, imaplib.IMAP4.error)
    # make sure pagination works
    assert list(inbox_source().uids) == list(inbox_source(chunksize=3).uids)


def test_load_attachments() -> None:
    _idx = -1
    filter_emails = ["josue@sehnem.com"]
    for _idx, item in enumerate(inbox_source(filter_emails=filter_emails).attachments):
        # Make sure just filtered emails are processed
        # print(item)
        # message fields in "message field"
        assert "josue@sehnem.com" in item["message"]["From"]
        if item["file_name"] == "sample.txt":
            # Find the attachment with the file name and assert the loaded content
            content = item.read_bytes()
            assert content == b"dlthub content"
            # make sure file item is right
            assert item["file_url"].startswith("imap://")
            assert item["mime_type"] == "text/plain"
            assert item["file_name"] == "sample.txt"
            assert isinstance(item["modification_date"], pendulum.DateTime)

    # two attachments expected
    assert _idx == 1

    # one text and one pdf expected
    filter_by_mime_type = ["application/pdf"]
    assert (
        len(
            list(
                inbox_source(
                    filter_emails=filter_emails, filter_by_mime_type=filter_by_mime_type
                ).attachments
            )
        )
        == 1
    )


def test_load_messages() -> None:
    _idx = -1
    for _idx, item in enumerate(
        inbox_source(filter_emails=("josue@sehnem.com",)).messages
    ):
        print(item)
        # Make sure just filtered emails are processed
        assert "josue@sehnem.com" in item["From"]
        if item["message_uid"] == 22:
            assert item["body"] == "test body\r\n"
            assert item["Subject"] == "test subject"

    assert _idx == 2


def test_parse_pdf() -> None:
    from sources.inbox_pipeline import imap_get_attachments

    # will find and load pdf
    pipeline = imap_get_attachments()

    # this is the only page we expect in data
    assert_query_data(pipeline, "SELECT text FROM my_pages", ["Dumm y PDF file"])
