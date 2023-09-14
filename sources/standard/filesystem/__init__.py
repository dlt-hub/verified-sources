"""This source collects fsspec files and downloads or proccess them."""
import mimetypes
import os
from typing import Optional

import dlt
from dlt.common import pendulum
from dlt.common.storages import filesystem
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.time import ensure_pendulum_datetime
from dlt.extract.source import DltResource, TDataItem, TDataItems

from ..file_source import FileModel
from .settings import DEFAULT_CHUNK_SIZE, DEFAULT_START_DATE, STORAGE_PATH

from fsspec import AbstractFileSystem
import pandas as pd


@dlt.source
def filesystem_source(
    credentials: FilesystemConfiguration = dlt.secrets.value,
    storage_path: str = STORAGE_PATH,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    filename_filter: Optional[str] = None,
    chunksize: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
) -> DltResource:
    """This source collect files and download or extract data from them.

    Args:
        credentials (FilesystemConfiguration): The credentials to the filesystem.
        storage_path (str, optional): The path to store the files.
        start_date (pendulum.DateTime, optional): The date to start collecting files.
        filename_filter (str, optional): The filter to apply to the files in glob format.
        chunksize (int, optional): The number of files to process at once.

    Returns:
        DltResource: The resource with the files.
    """

    bucket_url = credentials.bucket_url
    protocol = credentials.protocol
    fs_client, _ = filesystem(protocol=protocol, credentials=credentials)

    file_list = list_remote_files(
        fs_client,
        bucket_url,
        filename_filter,
        chunksize=chunksize,
        modified_at=dlt.sources.incremental(
            "mtime",
            initial_value=start_date,
            allow_external_schedulers=True,
        ),
    )

    if extract_content:
        return file_list | extract_files(fs_client=fs_client)
    else:
        return file_list | copy_files(fs_client=fs_client, storage_path=storage_path)


@dlt.resource
def list_remote_files(
    fs_client: AbstractFileSystem,
    bucket_url: str,
    filename_filter: Optional[str] = None,
    modified_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
        "mtime",
        initial_value=DEFAULT_START_DATE,
        allow_external_schedulers=True,
    ),
    chunksize: int = DEFAULT_CHUNK_SIZE,
) -> TDataItem:
    """List the files available in the filesystem.
    
    Args:
        fs_client (AbstractFileSystem): The filesystem client.
        bucket_url (str): The url to the bucket.
        filename_filter (Optional[str], optional): The filter to apply to the files in glob format.
        modified_at (dlt.sources.incremental[pendulum.DateTime], optional): The date to start collecting files.
        chunksize (int, optional): The number of files to process at once.

    Returns:
        TDataItem: The list of files.
    """

    if not filename_filter:
        filename_filter = "*"
    files_url = os.path.join(bucket_url, filename_filter)
    files = fs_client.glob(files_url, detail=True)

    last_modified = modified_at.last_value

    file_list = []
    for file, md in files.items():
        for dt_field in ["created", "mtime"]:
            md[dt_field] = ensure_pendulum_datetime(md[dt_field])
        # Skip files modified before last run
        if last_modified and md["mtime"] < last_modified:
            continue
        md["path"] = file
        md["content_type"] = mimetypes.guess_type(file)[0]
        file_list.append(md)

    for i in range(0, len(file_list), chunksize):
        yield file_list[i : i + chunksize]


@dlt.transformer(
    name="files",
    write_disposition="merge",
    merge_key="file_name",
    primary_key="file_name",
)
def copy_files(
    items: TDataItems,
    fs_client: AbstractFileSystem,
    storage_path: str = STORAGE_PATH,
) -> TDataItem:
    """Reads files and copy them to local directory.
    
    Args:
        items (TDataItems): The list of files to copy.
        fs_client (AbstractFileSystem): The filesystem client.
        storage_path (str, optional): The path to store the files.

    Returns:
        TDataItem: The list of files copied.
    """
    for file in items:
        file_path = file["path"]
        filename = os.path.basename(file_path)
        target_path = os.path.join(storage_path, filename)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        fs_client.cp(file_path, target_path)

        file_hash = fs_client.checksum(file_path)
        modified = fs_client.modified(file_path)

        file_md = FileModel(
            file_name=filename,
            file_path=os.path.abspath(target_path),
            content_type=file["content_type"],
            modification_date=ensure_pendulum_datetime(modified),
            data_hash=file_hash,
        )

        yield file_md.dict()


@dlt.transformer(
    name="content",
    write_disposition="merge",
    merge_key=None,
    primary_key=None,
)
def extract_files(
    items: TDataItems,
    fs_client: AbstractFileSystem,
    copy_before_parse: bool = False,
) -> TDataItem:
    """Reads files and copy them to local directory.
    
    Args:
        items (TDataItems): The list of files to copy.
        fs_client (AbstractFileSystem): The filesystem client.
        copy_before_parse (bool, optional): Copy the files to local directory before parsing.

    Returns:
        TDataItem: The file content
    """
    for file in items:
        if file["content_type"] == "text/csv":
            # TODO: Implement a way to pass kwargs.
            for df in pd.read_csv(file["path"], chunksize=100):
                yield df.to_dict(orient="records")
