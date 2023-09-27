import os
from typing import Any

import dlt
from dlt.common.time import ensure_pendulum_datetime
from pendulum.tz import UTC

try:
    from .standard.filesystem import filesystem_resource  # type: ignore
    from .standard.inbox import inbox_source  # type: ignore
except ImportError:
    from standard.filesystem import filesystem_resource
    from standard.inbox import inbox_source

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
            f.write(file_obj.read_bytes())
        yield file_obj


@dlt.transformer(
    table_name="met_data",
    merge_key="date",
    primary_key="date",
    write_disposition="merge",
)
def extract_met_csv(
    items: TDataItems,
    incremental: dlt.sources.incremental[str] = dlt.sources.incremental(
        "date",
        primary_key="date",
        initial_value="2023-01-01",
        allow_external_schedulers=True,
    ),
) -> TDataItem:
    """Reads files and copy them to local directory.

    Args:
        item (TDataItem): The list of files to copy.
        incremental (dlt.sources.incremental[str], optional): The incremental source.

    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        # Here we use pandas chunksize to read the file in chunks and avoid loading the whole file
        # in memory.
        for df in pd.read_csv(
            file_obj.open(),
            usecols=["code", "date", "temperature"],
            parse_dates=["date"],
            chunksize=15,
        ):
            last_value = ensure_pendulum_datetime(incremental.last_value)
            df["date"] = df["date"].apply(ensure_pendulum_datetime)
            df = df[df["date"] > last_value]
            yield df.to_dict(orient="records")


def imap_inbox() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="standard_inbox",
        destination="duckdb",
        dataset_name="standard_inbox_data",
        full_refresh=True,
    )

    data_source = inbox_source(
        filter_by_emails=("josue@sehnem.com",),
        attachments=True,
        chunksize=10,
        # filter_by_mime_type=("text/txt",),
    )

    data_resource = data_source.resources["attachments"] | copy_files(
        storage_path="standard/files"
    )
    # run the pipeline with your parameters
    load_info = pipeline.run(data_resource)
    # pretty print the information on data that was loaded
    print(load_info)

    filter_emails = ("astra92293@gmail.com", "josue@sehnem.com")

    data_source = inbox_source(
        filter_by_emails=filter_emails,
        attachments=False,
        chunksize=10,
    )
    data_resource = data_source.resources["messages"]
    # run the pipeline with your parameters
    load_info = pipeline.run(data_resource)
    # pretty print the information on data that was loaded
    print(load_info)


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


def read_csv_resource() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",
        destination="duckdb",
        dataset_name="met_data",
    )

    # When using the filesystem resource, you can specify a filter to select only the files you
    # want to load including a glob pattern. If you use a recursive glob pattern, the filenames
    # will include the path to the file inside the bucket_url.
    csv_source = (
        filesystem_resource(
            filename_filter="*/*.csv",
            extract_content=True,
        )
        | extract_met_csv
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(csv_source)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # copy_files_resource()
    # read_csv_resource()
    imap_inbox()
