"""This source collects inbox emails and downloads attachments to local folder"""
import hashlib
import mimetypes
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

import dlt
from dlt.common import logger, pendulum
from dlt.common.storages import filesystem
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.time import ensure_pendulum_datetime
from dlt.extract.source import DltResource, TDataItem, TDataItems

from ..file_source import FileModel
from .settings import DEFAULT_CHUNK_SIZE, DEFAULT_START_DATE, STORAGE_PATH


@dlt.source
def filesystem_source(
    credentials: FilesystemConfiguration = dlt.secrets.value,
    storage_path: str = STORAGE_PATH,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    chunksize: int = DEFAULT_CHUNK_SIZE,
) -> DltResource:
    """This source collects inbox emails and downloads attachments to the local folder."""

    file_list = list_remote_files(
        credentials,
        chunksize=chunksize,
    )

    yield from file_list | copy_files


@dlt.resource
def list_remote_files(
    credentials: FilesystemConfiguration = dlt.secrets.value,
    chunksize: int = DEFAULT_CHUNK_SIZE,
) -> TDataItem:
    """Collects email message UIDs (Unique IDs) from the mailbox."""

    bucket_url = credentials.bucket_url
    protocol = credentials.protocol
    fs_client, _ = filesystem(protocol=protocol, credentials=credentials)
    files = fs_client.ls(bucket_url)
    for file in files:
        yield [{"file_path": file}]


@dlt.transformer(
    name="files",
    write_disposition="merge",
    merge_key="filename",
    primary_key="filename",
)
def copy_files(
    items: TDataItems,
    storage_path: str = STORAGE_PATH,
    credentials: FilesystemConfiguration = dlt.secrets.value,
) -> TDataItem:
    """Reads files and copy them to local directory."""
    fs_client, _ = filesystem(credentials.protocol, credentials)
    for item in items:
        filename = os.path.basename(item["file_path"])
        target_path = os.path.join(storage_path, filename)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        fs_client.cp(item["file_path"], target_path)

        file_hash = fs_client.checksum(item["file_path"])
        modified = fs_client.modified(item["file_path"])

        file_md = FileModel(
            file_name=filename,
            file_path=target_path,
            content_type=mimetypes.guess_type(item["file_path"])[0],
            modification_date=ensure_pendulum_datetime(modified),
            data_hash=file_hash,
        )

        yield file_md.dict()
