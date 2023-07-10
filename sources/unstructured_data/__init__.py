"""This source converts unstructured data from a specified data resource to structured data using provided queries."""

import logging
import os
from pathlib import Path
from typing import Dict

import dlt
from dlt.extract.source import TDataItem

from .helpers import filetype_mapper, process_file_to_structured

logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)


@dlt.transformer
def unstructured_to_structured_resource(
    unstructured_item: TDataItem,
    queries: Dict[str, str],
    openai_api_key: str = dlt.secrets.value,
) -> TDataItem:
    """
    Converts unstructured data from a specified data item to structured data using provided queries.

    Args:
        unstructured_item (TDataItem): The data item containing unstructured data to be converted.
        queries (Dict[str, str]): A dictionary of queries to be applied to the unstructured data during processing.
            Each query maps a field name to a query string that specifies how to process the field.
        openai_api_key (str): The API key for the OpenAI API. If provided, it sets the `OPENAI_API_KEY` environment variable.
            Defaults to the value of `dlt.secrets.value`.

    Returns:
        TDataItem: The structured data item resulting from the conversion.

    """
    if openai_api_key:
        os.environ["OPENAI_API_KEY"] = openai_api_key

    file_path = unstructured_item["file_path"]
    extension = Path(file_path).suffix
    if extension in filetype_mapper:
        loader = filetype_mapper[extension](file_path)
        yield process_file_to_structured(loader, queries)
    else:
        logging.warning(
            f"Extension {extension} is not implemented, only ({', '.join(filetype_mapper.keys())}) are available."
        )

