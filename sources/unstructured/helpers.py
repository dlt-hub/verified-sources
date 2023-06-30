from typing import Dict, List

from langchain.document_loaders import UnstructuredPDFLoader, UnstructuredFileLoader
from langchain.indexes import VectorstoreIndexCreator


def safely_query_index(index, query):
    try:
        return index.query(query).strip()
    except Exception:
        return []


filetype_mapper = {
    ".txt": UnstructuredFileLoader,
    ".pdf": UnstructuredPDFLoader,
    }


def process_file_to_structured(loader, queries: Dict[str: str]) -> Dict:
    index = VectorstoreIndexCreator().from_loaders([loader])
    response = {"file_name": loader.file_path}
    for k, query in queries.items():
        response[k] = safely_query_index(index, query)

    return response