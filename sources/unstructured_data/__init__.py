import logging
import os
from pathlib import Path
from typing import Dict, Iterable

import dlt
from dlt.extract.source import DltResource, TDataItem

from .helpers import filetype_mapper

logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)


@dlt.source(name="unstructured")
def unstructured_source(
    data_resource: DltResource,
    queries: Dict[str, str],
    openai_api_key: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    if openai_api_key:
        os.environ["OPENAI_API_KEY"] = openai_api_key

    @dlt.transformer(
        data_from=data_resource,
        name=f"unstructured_data_from_{data_resource.name}",
    )
    def unstructured_resource(item: TDataItem) -> Iterable[TDataItem]:
        file_path = item["file_path"]
        extension = Path(file_path).suffix
        if extension in filetype_mapper:
            loader = filetype_mapper[extension](file_path)
            yield process_file_to_structured_mock(loader, queries)
        else:
            logging.warning(
                f"Extension {extension} is not implemented, only ({', '.join(filetype_mapper.keys())}) are available."
            )

    return data_resource, unstructured_resource


def process_file_to_structured_mock(loader, queries):
    response = {"file_path": loader.file_path}
    for k, query in queries.items():
        response[k] = query

    return response
