"""This source converts unstructured data from a specified data resource to structured data using provided queries."""

import logging
import os
from pathlib import Path
from typing import Dict, Iterable

import dlt
from dlt.extract.source import DltResource, TDataItem

from .helpers import filetype_mapper, process_file_to_structured

logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)


@dlt.source(name="unstructured_to_structured")
def unstructured_to_structured_source(
    unstructured_resource: DltResource,
    queries: Dict[str, str],
    openai_api_key: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    Converts unstructured data from a specified data resource to structured data using provided queries.

    Args:
        unstructured_resource (DltResource): The data resource containing unstructured data to be converted.
        queries (Dict[str, str]): A dictionary of queries to be applied to the unstructured data during processing.
            Each query maps a field name to a query string that specifies how to process the field.
        openai_api_key (str): The API key for the OpenAI API. If provided, it sets the `OPENAI_API_KEY` environment variable.
            Defaults to the value of `dlt.secrets.value`.

    Returns:
        Iterable[DltResource]: An iterable containing a resource with original unstructured data and resource with structured data.

    """
    if openai_api_key:
        os.environ["OPENAI_API_KEY"] = openai_api_key

    @dlt.transformer(
        data_from=unstructured_resource,
        name=f"structured_data_from_{unstructured_resource.name}",
    )
    def structured_resource(unstructured_item: TDataItem) -> Iterable[TDataItem]:
        file_path = unstructured_item["file_path"]
        extension = Path(file_path).suffix
        if extension in filetype_mapper:
            loader = filetype_mapper[extension](file_path)
            yield process_file_to_structured(loader, queries)
        else:
            logging.warning(
                f"Extension {extension} is not implemented, only ({', '.join(filetype_mapper.keys())}) are available."
            )

    return unstructured_resource, structured_resource  # type: ignore
