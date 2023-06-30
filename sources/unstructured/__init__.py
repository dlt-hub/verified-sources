import dlt
import os
from typing import Dict, List, Sequence
from pathlib import Path
from dlt.extract.source import DltResource, DltSource
from .helpers import process_file_to_structured, filetype_mapper


@dlt.source(name="filesystem")
def filesystem_source(data_dir=dlt.secrets.value, extensions: Sequence = (".txt",)) -> DltResource:

    @dlt.resource(write_disposition="replace")
    def local_folder():
        files = (p.resolve() for p in Path(data_dir).glob("**/*") if p.suffix in set(extensions))
        for file in files:
           yield file

    return local_folder


@dlt.source(name="unstructured")
def unstructured_source(data_resource: DltResource, queries: Dict[str, str]) -> DltResource:

    @dlt.transformer(
        data_from=data_resource,
        name=f"unstructured_data_from_{data_resource.name}"
    )
    def unstructured_resource(file_path):
        extension = file_path.suffix
        loader = filetype_mapper[extension](file_path)
        yield process_file_to_structured(loader, queries)

    return unstructured_resource
