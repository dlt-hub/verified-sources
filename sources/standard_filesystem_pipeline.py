import os
from io import BytesIO
from typing import Any

import dlt

try:
    from .standard.filesystem import filesystem_resource  # type: ignore
except ImportError:
    from standard.filesystem import filesystem_resource

import pandas as pd
from dlt.extract.source import TDataItem, TDataItems


def read_file(file_data: TDataItem) -> Any:
    """Reads a file from filesystem resource and return the bytes.

    Args:
        file_data (TDataItem): The file to read.

    Returns:
        bytes: The file content
    """
    if "content" in file_data:
        content = file_data.pop("content")
    elif "file_instance" in file_data:
        content = file_data.pop("file_instance").read()
    return content


@dlt.transformer(name="filesystem")
def copy_files(
    items: TDataItems,
    storage_path: str,
) -> TDataItem:
    """Reads files and copy them to local directory.

    Args:
        items (TDataItems): The list of files to copy.
        storage_path (str, optional): The path to store the files.

    Returns:
        TDataItem: The list of files copied.
    """
    storage_path = os.path.abspath(storage_path)
    os.makedirs(storage_path, exist_ok=True)
    for file_md in items:
        file_dst = os.path.join(storage_path, file_md["file_name"])
        file_md["path"] = file_dst
        with open(file_dst, "wb") as f:
            f.write(read_file(file_md))
        yield file_md


@dlt.transformer(
    name="filesystem",
    write_disposition="merge",
    merge_key=None,
    primary_key=None,
)
def extract_csv(
    items: TDataItems,
) -> TDataItem:
    """Reads files and copy them to local directory.

    Args:
        items (TDataItems): The list of files to copy.

    Returns:
        TDataItem: The file content
    """
    for file_md in items:
        file_data = BytesIO(read_file(file_md))
        for df in pd.read_csv(file_data, chunksize=100):
            yield df.to_dict(orient="records")


def from_standard_filesystem() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",
        destination="duckdb",
        dataset_name="standard_filesystem_data",
        full_refresh=True,
    )

    file_source = filesystem_resource(
        filename_filter="mlb*.csv",
        chunksize=1,
        extract_content=False,
    ) | copy_files(storage_path="standard/files")

    # run the pipeline with your parameters
    load_info = pipeline.run(file_source)
    # pretty print the information on data that was loaded
    print(load_info)

    csv_source = (
        filesystem_resource(
            filename_filter="mlb*.csv",
            chunksize=10,
        )
        | extract_csv
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(csv_source)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    from_standard_filesystem()
