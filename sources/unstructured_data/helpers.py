from typing import Any, Dict, List, Type, Union

from langchain.document_loaders import UnstructuredFileLoader
from langchain.indexes import VectorstoreIndexCreator
from langchain.vectorstores.base import VectorStore
from langchain.vectorstores.chroma import Chroma
from langchain.vectorstores.elastic_vector_search import ElasticVectorSearch
from langchain.vectorstores.weaviate import Weaviate


vectorstore_mapping = {
    "chroma": Chroma,
    "elastic_search": ElasticVectorSearch,
    "weaviate": Weaviate,
}


def safely_query_index(index: Any, query: str) -> Any:
    return index.query(query).strip()


def process_file_to_structured(
    file_path: Union[str, List[str]],
    queries: Dict[str, str],
    vectorstore: Type[VectorStore] = Chroma,
) -> Dict[str, Any]:
    """
    Processes a file loaded by the specified loader and generates structured data based on provided queries.

    Args:
        file_path (Union[str, List[str]]): filepath to the file with unstructured data.
        queries (Dict[str, str]): A dictionary of queries to be applied to the loaded file.
            Each query maps a field name to a query string that specifies how to process the field.
        vectorstore (Type[VectorStore]): Vector database type. Subclass of VectorStore. Default to Chroma.

    Returns:
        Dict[str, str]: A dictionary containing the processed structured data from the loaded file.
            The dictionary includes a "file_path" key with the path of the loaded file and
            additional keys corresponding to the queried fields and their processed values.
    """
    loader = UnstructuredFileLoader(file_path)
    index = VectorstoreIndexCreator(vectorstore_cls=vectorstore).from_loaders([loader])
    response = {"file_path": file_path}
    for k, query in queries.items():
        response[k] = safely_query_index(index, query)
    return response
