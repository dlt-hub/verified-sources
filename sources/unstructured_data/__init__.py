import dlt
import os
from typing import Dict, List, Sequence, Iterable
from pathlib import Path
from dlt.extract.source import DltResource, DltSource, TDataItem
from .helpers import process_file_to_structured, filetype_mapper


@dlt.source(name="unstructured")
def unstructured_source(
    data_resource: DltResource,
    queries: Dict[str, str],
    openai_api_key: str = dlt.secrets.value,
) -> Iterable[DltResource]:

    if openai_api_key:
        os.environ['OPENAI_API_KEY'] = openai_api_key

    @dlt.transformer(
        data_from=data_resource,
        name=f"unstructured_data_from_{data_resource.name}",
    )
    def unstructured_resource(item: TDataItem) -> Iterable[TDataItem]:
        file_path = item["file_path"]
        extension = Path(file_path).suffix
        loader = filetype_mapper[extension](file_path)
        yield process_file_to_structured(loader, queries)

    return data_resource, unstructured_resource


def process_file_to_structured_mock(loader, queries):
    response = {"file_path": loader.file_path}
    for k, query in queries.items():
        response[k] = query

    return response
