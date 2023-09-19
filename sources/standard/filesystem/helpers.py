"""Helpers for the filesystem resource."""
import mimetypes
import os
from typing import Iterable, Optional

import pendulum
from dlt.common.configuration.specs import configspec
from dlt.common.storages import filesystem_from_config
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.time import ensure_pendulum_datetime
from fsspec import AbstractFileSystem  # type: ignore

from ..file_source import FileModel


@configspec
class FilesystemConfigurationResource(FilesystemConfiguration):
    storage_path: Optional[str]
    start_date: str


def client_from_credentials(credentials: FilesystemConfiguration) -> AbstractFileSystem:
    """Create a filesystem client from the credentials.
    
    Args:
        credentials (FilesystemConfiguration): The credentials to the filesystem.

    Returns:
        AbstractFileSystem: The filesystem client.
    """
    fs_client, _ = filesystem_from_config(credentials)
    return fs_client


def get_files(fs_client: AbstractFileSystem, files_url: str) -> Iterable[FileModel]:
    """Get the files from the filesystem client.

    Args:
        fs_client (AbstractFileSystem): The filesystem client.
        files_url (str): The url to the files.

    Returns:
        Iterable[FileModel]: The list of files.
    """
    files = fs_client.glob(files_url, detail=True)

    for file, md in files.items():
        for dt_field in ["created", "mtime"]:
            md[dt_field] = ensure_pendulum_datetime(md[dt_field])
        yield FileModel(
            file_name=os.path.basename(file),
            file_url=file,
            content_type=mimetypes.guess_type(file)[0],
            modification_date=md.get("mtime", md.get("created")) or pendulum.now(),
            size_in_bytes=md["size"],
        )
