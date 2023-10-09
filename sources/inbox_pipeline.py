from typing import Iterator, Sequence, Dict, Any
from PyPDF2 import PdfReader

import dlt

try:
    from .inbox import inbox_source, FileItemDict  # type: ignore
except ImportError:
    from inbox import inbox_source, FileItemDict


@dlt.transformer(primary_key="file_hash", write_disposition="merge")
def pdf_to_text(file_items: Sequence[FileItemDict]) -> Iterator[Dict[str, Any]]:
    # extract data from PDF page by page
    for file_item in file_items:
        with file_item.open() as file:
            reader = PdfReader(file)
            for page_no in range(len(reader.pages)):
                # add page content to file item
                page_item = {}
                page_item["file_hash"] = file_item["file_hash"]
                page_item["text"] = reader.pages[page_no].extract_text()
                page_item["subject"] = file_item["message"]["Subject"]
                page_item["page_id"] = file_item["file_name"] + "_" + str(page_no)
                # TODO: copy more info from file_item
                yield page_item


def imap_read_messages() -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        pipeline_name="standard_inbox_message_2",
        destination="duckdb",
        dataset_name="standard_inbox_data",
        full_refresh=True,
    )

    # get messages resource from the source
    messages = inbox_source(
        filter_emails=("astra92293@gmail.com", "josue@sehnem.com")
    ).messages
    # configure the messages resource to not get bodies of the messages
    messages = messages(include_body=False).with_name("my_inbox")
    # load messages to "my_inbox" table
    load_info = pipeline.run(messages)
    # pretty print the information on data that was loaded
    print(load_info)
    return pipeline


def imap_get_attachments() -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        pipeline_name="standard_inbox_attachments",
        destination="duckdb",
        dataset_name="standard_inbox_data",
        full_refresh=True,
    )

    # get attachment resource from a source, we only want pdfs that we later parse
    filter_emails = ["josue@sehnem.com"]
    attachments = inbox_source(
        filter_emails=filter_emails, filter_by_mime_type=["application/pdf"]
    ).attachments

    # feed attachments into pdf parser and load text into my_pages table
    load_info = pipeline.run((attachments | pdf_to_text).with_name("my_pages"))
    # pretty print the information on data that was loaded
    print(load_info)
    return pipeline


if __name__ == "__main__":
    imap_read_messages()
    imap_get_attachments()
