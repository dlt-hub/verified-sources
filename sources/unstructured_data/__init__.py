"""This source converts unstructured data from a specified data resource to structured data using provided queries."""

import asyncio
import os
from typing import Dict

import dlt
from dlt.common import logger
from dlt.extract.source import DltResource, TDataItem

from .helpers import (
    aprocess_file_to_structured,
    process_file_to_structured,
    vectorstore_mapping,
)


@dlt.resource
def unstructured_to_structured_resource(
    queries: Dict[str, str],
    openai_api_key: str = dlt.secrets.value,
    vectorstore: str = dlt.config.value,
    table_name: str = "unstructured_to_structured_resource",
    run_async: bool = False,
) -> DltResource:
    """
    Converts unstructured data from a specified data item to structured data using provided queries.

    Args:
        queries (Dict[str, str]): A dictionary of queries to be applied to the unstructured data during processing.
            Each query maps a field name to a query string that specifies how to process the field.
        openai_api_key (str): The API key for the OpenAI API. If provided, it sets the `OPENAI_API_KEY` environment variable.
            Defaults to the value of `dlt.secrets.value`.
        vectorstore (str): Vector database type, e.g. "chroma", "weaviate" or "elastic_search".
            Defaults to the value of `dlt.config.value`.
        table_name (str): The name of the table associated with the resource. Defaults to "unstructured_to_structured_resource".
        run_async (bool): Whether to run the conversion asynchronously. Defaults to False.

    Returns:
        DltResource: A resource-transformer object representing the conversion of unstructured data to structured data.

    """
    if openai_api_key:
        os.environ["OPENAI_API_KEY"] = openai_api_key

    return dlt.transformer(convert_data, name=table_name)(
        queries, vectorstore, run_async
    )


def convert_data(
    unstructured_item: TDataItem,
    queries: Dict[str, str],
    vectorstore: str = "chroma",
    run_async: bool = False,
) -> TDataItem:
    """
    Converts unstructured data item to structured data item using provided queries.

    Args:
        unstructured_item (TDataItem): The data item containing unstructured data to be converted.
        queries (Dict[str, str]): A dictionary of queries to be applied to the unstructured data during processing.
            Each query maps a field name to a query string that specifies how to process the field.
        vectorstore (str): Vector database type, e.g. "chroma", "weaviate" or "elastic_search". Default to "chroma".
        run_async (bool): Whether to run the conversion asynchronously. Defaults to False.
    Returns:
        TDataItem: The structured data item resulting from the conversion.

    """
    if run_async:
        logger.info("Run conversion asynchronously.")
        response = asyncio.run(
            aprocess_file_to_structured(
                unstructured_item["file_path"],
                queries,
                vectorstore_mapping[vectorstore],
            )
        )
    else:
        response = process_file_to_structured(
            unstructured_item["file_path"],
            queries,
            vectorstore_mapping[vectorstore],
        )

    response["file_path"] = unstructured_item.pop("file_path")
    response["metadata"] = unstructured_item
    yield response
