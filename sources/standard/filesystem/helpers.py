"""Helpers for the filesystem resource."""
import os
import mimetypes
import pendulum
from dlt.common.storages import filesystem
from dlt.common.configuration.specs import configspec
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.time import ensure_pendulum_datetime
from fsspec import AbstractFileSystem

from typing import Optional
from fsspec import AbstractFileSystem
from ..file_source import FileModel


@configspec
class FilesystemConfigurationResource(FilesystemConfiguration):
    storage_path: Optional[str]
    start_date: str


def client_from_credentials(credentials: FilesystemConfiguration) -> AbstractFileSystem:
    fs_client, _ = filesystem(credentials.protocol, credentials=credentials)
    return fs_client


def get_files(fs_client: AbstractFileSystem, files_url: str) -> FileModel:

    files = fs_client.glob(files_url, detail=True)

    for file, md in files.items():
        for dt_field in ["created", "mtime"]:
            md[dt_field] = ensure_pendulum_datetime(md[dt_field])
        file_dict = {
            "file_name": os.path.basename(file),
            "file_url": file,
            "content_type": mimetypes.guess_type(file)[0],
            "modification_date": md.get("mtime", md.get("created")) or pendulum.now(),
            "size_in_bytes": md["size"],
        }
        yield FileModel(**file_dict)

