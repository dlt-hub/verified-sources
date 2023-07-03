import logging
from typing import Dict, Optional

from langchain.document_loaders import UnstructuredFileLoader, UnstructuredPDFLoader
from langchain.indexes import VectorstoreIndexCreator

logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)

filetype_mapper = {
    ".txt": UnstructuredFileLoader,
    ".pdf": UnstructuredPDFLoader,
}


def safely_query_index(index, query: str) -> Optional[str]:
    try:
        answer = index.query(query).strip()
        return answer
    except Exception as e:
        logging.warning(e)
        return None


def process_file_to_structured(loader, queries: Dict[str, str]) -> Dict:
    index = VectorstoreIndexCreator().from_loaders([loader])
    response = {"file_name": loader.file_path}
    for k, query in queries.items():
        response[k] = safely_query_index(index, query)
    return response
