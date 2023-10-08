import json
import os
import posixpath
from typing import Any, Iterable
import pandas as pd
import pyarrow.parquet as pq  # type: ignore

import dlt

try:
    from .standard.filesystem import FileSystemDict, filesystem  # type: ignore
    from .standard.inbox import get_attachments, get_message_content, messages_uids  # type: ignore
except ImportError:
    from standard.filesystem import FileSystemDict, filesystem
    from standard.inbox import messages_uids, get_attachments, get_message_content


from dlt.extract.source import TDataItem

TESTS_BUCKET_URL = posixpath.abspath("../tests/standard/samples/")


@dlt.transformer(standalone=True)
def read_csv(
    items: Iterable[FileSystemDict],
    chunksize: int = 15,
):
    """Reads csv file with Pandas chunk by chunk.

    Args:
        item (TDataItem): The list of files to copy.
        chunksize (int): Number of records to read in one chunk
    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        # Here we use pandas chunksize to read the file in chunks and avoid loading the whole file
        # in memory.
        with file_obj.open() as file:
            for df in pd.read_csv(
                file,
                header="infer",
                chunksize=chunksize,
            ):
                yield df.to_dict(orient="records")


@dlt.transformer(standalone=True)
def read_jsonl(
    items: Iterable[FileSystemDict],
    chunksize: int = 10,
) -> TDataItem:
    """Reads jsonl file content and extract the data.

    Args:
        item (Iterable[FileSystemDict]): The list of files to copy.
        chunksize (int, optional): The number of files to process at once, defaults to 10.

    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        with file_obj.open() as f:
            lines_chunk = []
            for line in f:
                lines_chunk.append(json.loads(line))
                if len(lines_chunk) >= chunksize:
                    yield lines_chunk
                    lines_chunk = []
        if lines_chunk:
            yield lines_chunk


@dlt.transformer(standalone=True)
def read_parquet(
    items: Iterable[FileSystemDict],
    chunksize: int = 10,
) -> TDataItem:
    """Reads parquet file content and extract the data.

    Args:
        item (Iterable[FileSystemDict]): The list of files to copy.
        chunksize (int, optional): The number of files to process at once, defaults to 10.

    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        with file_obj.open() as f:
            parquet_file = pq.ParquetFile(f)
            for rows in parquet_file.iter_batches(batch_size=chunksize):
                yield rows.to_pylist()


def copy_files_resource(local_folder: str) -> None:
    """Demonstrates how to copy files locally by adding a step to filesystem resource and the to load the download listing to db"""
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem_copy",
        destination="duckdb",
        dataset_name="standard_filesystem_data",
    )

    # a step that copies files into test storage
    def _copy(item: FileSystemDict):
        # instantiate fsspec and copy file
        dest_file = os.path.join(local_folder, item["file_name"])
        # create dest folder
        os.makedirs(os.path.dirname(dest_file), exist_ok=True)
        # download file
        item.filesystem.download(item["file_url"], dest_file)
        # return file item unchanged
        return item

    # use recursive glob pattern and add file copy step
    downloader = filesystem(TESTS_BUCKET_URL, file_glob="**").add_map(_copy)

    # NOTE: you do not need to load any data to execute extract, below we obtain
    # a list of files in a bucket and also copy them locally
    listing = list(downloader)
    print(listing)

    # download to table "listing"
    # downloader = filesystem(TESTS_BUCKET_URL, file_glob="**").add_map(_copy)
    load_info = pipeline.run(downloader.with_name("listing"), write_disposition="replace")
    # pretty print the information on data that was loaded
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


def stream_and_merge_csv() -> None:
    """Demonstrates how to scan folder with csv files, load them in chunk and merge on date column with the next load"""
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem_csv",
        destination="duckdb",
        dataset_name="met_data",
    )
    # met_data contains 3 columns, where "date" column contain a date on which we want to merge
    # load all csvs in A801
    met_files = filesystem(bucket_url=TESTS_BUCKET_URL, file_glob="met_csv/A801/*.csv") | read_csv()
    # tell dlt to merge on date
    met_files.apply_hints(write_disposition="merge", merge_key="date")
    # NOTE: we load to met_csv table
    load_info = pipeline.run(
        met_files.with_name("met_csv")
    )
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)

    # now let's simulate loading on next day. not only current data appears but also updated record for the previous day are present
    # all the records for previous day will be replaced with new records
    met_files = filesystem(bucket_url=TESTS_BUCKET_URL, file_glob="met_csv/A803/*.csv") | read_csv()
    met_files.apply_hints(write_disposition="merge", merge_key="date")
    load_info = pipeline.run(
        met_files.with_name("met_csv")
    )

    # you can also do dlt pipeline standard_filesystem_csv show to confirm that all A801 were replaced with A803 records for overlapping day
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


def read_parquet_and_jsonl_chunked() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",
        destination="duckdb",
        dataset_name="teams_data",
    )
    # When using the filesystem resource, you can specify a filter to select only the files you
    # want to load including a glob pattern. If you use a recursive glob pattern, the filenames
    # will include the path to the file inside the bucket_url.

    # JSONL reading
    jsonl_reader = filesystem(TESTS_BUCKET_URL, file_glob="**/*.jsonl") | read_jsonl()
    # PARQUET reading
    parquet_reader = filesystem(TESTS_BUCKET_URL, file_glob="**/*.parquet") | read_parquet()
    # load both folders together to specified tables
    load_info = pipeline.run([
        jsonl_reader.with_name("jsonl_team_data"),
        parquet_reader.with_name("parquet_team_data")
    ])
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


def read_files_incrementally_mtime() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem_incremental",
        destination="duckdb",
        dataset_name="file_tracker",
    )

    # here we modify filesystem resource so it will track only new csv files
    # such resource may be then combined with transformer doing further processing
    new_files = filesystem(bucket_url=TESTS_BUCKET_URL, file_glob="csv/*")
    # add incremental on modification time
    new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run((new_files | read_csv()) .with_name("csv_files"))
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)

    # load again - no new files!
    new_files = filesystem(bucket_url=TESTS_BUCKET_URL, file_glob="csv/*")
    # add incremental on modification time
    new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run((new_files | read_csv()) .with_name("csv_files"))
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


# IMAP examples
def imap_read_messages() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_inbox",
        destination="duckdb",
        dataset_name="standard_inbox_data",
        full_refresh=True,
    )

    filter_emails = ("astra92293@gmail.com", "josue@sehnem.com")
    data_source = messages_uids(filter_emails=filter_emails)

    messages = data_source | get_message_content(include_body=True)

    # run the pipeline with your parameters
    load_info = pipeline.run(messages)
    # pretty print the information on data that was loaded
    print(load_info)


def imap_get_attachments() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_inbox",
        destination="duckdb",
        dataset_name="standard_inbox_data",
        full_refresh=True,
    )

    filter_emails = ("josue@sehnem.com",)
    data_source = messages_uids(filter_emails=filter_emails)

    attachments = (
        data_source | get_attachments | copy_files(storage_path="standard/files")
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(attachments)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    copy_files_resource("_storage")
    # stream_and_merge_csv()
    # read_parquet_and_jsonl_chunked()
    # read_files_incrementally_mtime()
    # read_file_content_resource()
    # imap_read_messages()
    # imap_get_attachments()
