from typing import Dict, List

from langchain.document_loaders import UnstructuredPDFLoader, UnstructuredFileLoader
from langchain.indexes import VectorstoreIndexCreator


def safely_query_index(index, query):
    try:
        return index.query(query).strip()
    except Exception:
        return []


def process_one_pdf_to_structured(path_to_pdf:str) -> Dict:
    loader = UnstructuredPDFLoader(path_to_pdf)
    return process_file_to_structured(loader, path_to_pdf)


def process_one_txt_to_structured(path_to_file:str) -> Dict:
    loader = UnstructuredFileLoader(path_to_file)
    return process_file_to_structured(loader, path_to_file)


def process_file_to_structured(loader, path_to_file) -> Dict:
    index = VectorstoreIndexCreator().from_loaders([loader])
    return {
        "file_name": path_to_file.split("/")[-1],
        "recipient_company_name": safely_query_index(index, "Who is the recipient of the invoice? Just return the name"),
        "invoice_amount": safely_query_index(index, "What is the total amount of the invoice? Just return the amount as decimal number, no currency or text"),
        "invoice_date": safely_query_index(index, "What is the date of the invoice? Just return the date"),
        "invoice_number": safely_query_index(index, "What is the invoice number? Just return the number"),
        "service_description": safely_query_index(index, "What is the description of the service that this invoice is for? Just return the description"),
    }