import dlt
import os
from typing import Dict, List, Sequence
from pathlib import Path
from dlt.extract.source import DltResource, DltSource
from .helpers import process_one_pdf_to_structured, process_one_txt_to_structured


@dlt.source(name="filesystem")
def filesystem_source(data_dir=dlt.secrets.value, extensions: Sequence = (".txt",)) -> DltResource:

    @dlt.resource(write_disposition="replace")
    def local_folder():
        files = (p.resolve() for p in Path(data_dir).glob("**/*") if p.suffix in set(extensions))
        for file in files:
           yield file

    return local_folder


@dlt.source(name="unstructured")
def unstructured_source(data_resource: DltResource) -> DltResource:

    @dlt.transformer(
        data_from=data_resource,
        name=f"unstructured_data_from_{data_resource.name}"
    )
    def unstructured_resource(file_path):
        if file_path.suffix in (".pdf",):
            yield process_one_pdf_to_structured(path_to_pdf=file_path)
        elif file_path.suffix in (".txt",):
            yield process_one_txt_to_structured(path_to_file=file_path)
        else:
            NotImplementedError()

    return unstructured_resource
