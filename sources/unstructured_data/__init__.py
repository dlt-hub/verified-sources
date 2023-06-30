import dlt
import os
from typing import Dict, List, Sequence, Iterable
from pathlib import Path
from dlt.extract.source import DltResource, DltSource, TDataItem
from .helpers import process_file_to_structured, filetype_mapper


@dlt.resource(write_disposition="replace")
def local_folder(data_dir=dlt.secrets.value, extensions: Sequence = (".txt",)):
    files = (p.resolve() for p in Path(data_dir).glob("**/*") if p.suffix in set(extensions))
    for file in files:
        yield {"filepath": str(file)}


@dlt.source(name="unstructured", max_table_nesting=3)
def unstructured_source(queries: Dict[str, str], openai_api_key = dlt.secrets.value,) -> Iterable[DltResource]:
    if openai_api_key:
        os.environ['OPENAI_API_KEY'] = openai_api_key

    @dlt.transformer(
        data_from=local_folder,
        name=f"unstructured_data_from_{local_folder.name}",
        write_disposition='replace',
    )
    def unstructured_resource(item) -> Iterable[TDataItem]:
        file_path = item["filepath"]
        extension = Path(file_path).suffix
        loader = filetype_mapper[extension](file_path)
        yield process_file_to_structured_mock(loader, queries)

    return local_folder, unstructured_resource


def process_file_to_structured_mock(loader, queries):
    response = {}
    for k, query in queries.items():
        response[k] = query

    return response
