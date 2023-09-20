"""Helpers for the filesystem resource."""
import mimetypes
import os
from typing import Iterable, Optional
from urllib.parse import urlparse

from dlt.common.configuration.specs import configspec
from dlt.common.storages import filesystem
from dlt.common.storages.configuration import (
    FilesystemConfiguration,
    FileSystemCredentials,
)
from dlt.common.storages.filesystem import MTIME_DISPATCH, FileItem
from fsspec import AbstractFileSystem  # type: ignore
from pendulum import DateTime


@configspec
class FilesystemConfigurationResource(FilesystemConfiguration):
    storage_path: Optional[str]
    start_date: Optional[DateTime]


def client_from_credentials(
    bucket_url: str, credentials: FileSystemCredentials
) -> AbstractFileSystem:
    """Create a filesystem client from the credentials.

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials): The credentials to the filesystem.

    Returns:
        AbstractFileSystem: The filesystem client.
    """
    fs_client, _ = filesystem(bucket_url, credentials)
    return fs_client


def get_files(fs_client: AbstractFileSystem, files_url: str) -> Iterable[FileItem]:
    """Get the files from the filesystem client.

    Args:
        fs_client (AbstractFileSystem): The filesystem client.
        files_url (str): The url to the files.

    Returns:
        Iterable[FileItem]: The list of files.
    """

    protocol = urlparse(files_url).scheme or "file"
    files = fs_client.glob(files_url, detail=True)

    for file, md in files.items():
        file_name = os.path.basename(file)
        file_url = os.path.join(os.path.dirname(files_url), file_name)
        yield FileItem(
            file_name=file_name,
            file_url=file_url,
            mime_type=mimetypes.guess_type(file)[0],
            modification_date=MTIME_DISPATCH[protocol](md),
            file_content=None,
        )
