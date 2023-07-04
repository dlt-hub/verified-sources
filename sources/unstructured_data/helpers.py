import logging
from typing import Dict, Optional, Any

from langchain.document_loaders import UnstructuredFileLoader, UnstructuredPDFLoader
from langchain.indexes import VectorstoreIndexCreator

logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)

filetype_mapper = {
    ".txt": UnstructuredFileLoader,
    ".pdf": UnstructuredPDFLoader,
}


def safely_query_index(index: Any, query: str) -> Any:
    try:
        answer = index.query(query).strip()
        return answer
    except Exception as e:
        logging.warning(e)
        return None


def process_file_to_structured(loader: Any, queries: Dict[str, str]) -> Dict[str, Any]:
    """
    Processes a file loaded by the specified loader and generates structured data based on provided queries.

    Args:
        loader: (Any) The loader that uses unstructured to load files. E.g. UnstructuredFileLoader, UnstructuredPDFLoader.
        queries (Dict[str, str]): A dictionary of queries to be applied to the loaded file.
            Each query maps a field name to a query string that specifies how to process the field.

    Returns:
        Dict[str, str]: A dictionary containing the processed structured data from the loaded file.
            The dictionary includes a "file_path" key with the path of the loaded file and
            additional keys corresponding to the queried fields and their processed values.
    """
    index = VectorstoreIndexCreator().from_loaders([loader])
    response = {"file_path": loader.file_path}
    for k, query in queries.items():
        response[k] = safely_query_index(index, query)
    return response
