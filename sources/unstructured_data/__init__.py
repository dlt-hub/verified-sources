"""This source converts unstructured data from a specified data resource to structured data using provided queries."""

import logging
import os
from pathlib import Path
from typing import Dict

import dlt
from dlt.extract.source import TDataItem, DltResource

from .helpers import filetype_mapper, process_file_to_structured

logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)


@dlt.resource
def unstructured_to_structured_resource(
    queries: Dict[str, str],
    openai_api_key: str = dlt.secrets.value,
    table_name: str = "unstructured_to_structured_resource",
) -> DltResource:
    """
    Converts unstructured data from a specified data item to structured data using provided queries.

    Args:
        queries (Dict[str, str]): A dictionary of queries to be applied to the unstructured data during processing.
            Each query maps a field name to a query string that specifies how to process the field.
        openai_api_key (str): The API key for the OpenAI API. If provided, it sets the `OPENAI_API_KEY` environment variable.
            Defaults to the value of `dlt.secrets.value`.
        table_name (str): The name of the table associated with the resource. Defaults to "unstructured_to_structured_resource".

    Returns:
        DltResource: A resource-transformer object representing the conversion of unstructured data to structured data.

    """
    if openai_api_key:
        os.environ["OPENAI_API_KEY"] = openai_api_key

    return dlt.transformer(convert_data, name=table_name)(queries)


def convert_data(item: TDataItem, queries: Dict[str, str]) -> TDataItem:
    """
    Converts unstructured data item to structured data item using provided queries.

    Args:
        item (TDataItem): The data item containing unstructured data to be converted.
        queries (Dict[str, str]): A dictionary of queries to be applied to the unstructured data during processing.
            Each query maps a field name to a query string that specifies how to process the field.

    Returns:
        TDataItem: The structured data item resulting from the conversion.

    """
    file_path = item["file_path"]
    extension = Path(file_path).suffix
    if extension in filetype_mapper:
        loader = filetype_mapper[extension](file_path)
        yield process_file_to_structured(loader, queries)
    else:
        logging.warning(
            f"Extension {extension} is not implemented, only ({', '.join(filetype_mapper.keys())}) are available."
        )
