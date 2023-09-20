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
    for file_obj in items:
        file_dst = os.path.join(storage_path, file_obj["file_name"])
        file_obj["path"] = file_dst
        with open(file_dst, "wb") as f:
            f.write(file_obj.read())
        yield file_obj


@dlt.transformer(
    name="filesystem",
    write_disposition="merge",
    merge_key=None,
    primary_key=None,
)
def extract_parquet(
    items: TDataItems,
) -> TDataItem:
    """Reads files and copy them to local directory.

    Args:
        items (TDataItems): The list of files to copy.

    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        df = pd.read_parquet(file_obj.open_fs())
        yield df.to_dict(orient="records")


def copy_files_resource() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",
        destination="duckdb",
        dataset_name="standard_filesystem_data",
        full_refresh=True,
    )

    file_source = filesystem_resource(
        chunksize=10,
        extract_content=True,
    ) | copy_files(storage_path="standard/files")

    # run the pipeline with your parameters
    load_info = pipeline.run(file_source)
    # pretty print the information on data that was loaded
    print(load_info)


def read_parquet_resource() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",
        destination="duckdb",
        dataset_name="standard_filesystem_data",
        full_refresh=True,
    )

    parquet_source = (
        filesystem_resource(
            filename_filter="*e.parquet",
        )
        | extract_parquet
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(parquet_source)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",
        destination="duckdb",
        dataset_name="standard_filesystem_data",
        full_refresh=True,
    )

    copy_files_resource()
    # read_parquet_resource()
