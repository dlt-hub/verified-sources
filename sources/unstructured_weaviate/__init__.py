"""This source converts unstructured data from a specified data resource to structured data using provided queries."""
import dlt

from PyPDF2 import PdfReader

from .settings import INVOICE_QUERIES


@dlt.transformer(primary_key="page_id", write_disposition="merge")
def pdf_to_text(file_item, separate_pages: bool = False):
    if not separate_pages:
        raise NotImplementedError()
    # extract data from PDF page by page
    reader = PdfReader(file_item["file_path"])
    for page_no in range(len(reader.pages)):
        # add page content to file item
        page_item = dict(file_item)
        page_item["text"] = reader.pages[page_no].extract_text()
        page_item["page_id"] = file_item["file_name"] + "_" + str(page_no)
        yield page_item
