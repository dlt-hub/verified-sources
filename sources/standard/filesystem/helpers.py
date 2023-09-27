"""Helpers for the filesystem resource."""
import mimetypes
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


class FilesystemFileItem(FileItem):
    """A file item with size in bytes."""

    size_in_bytes: int


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


def get_files(
    fs_client: AbstractFileSystem, bucket_url: str, filename_filter: str
) -> Iterable[FileItem]:
    """Get the files from the filesystem client.

    Args:
        fs_client (AbstractFileSystem): The filesystem client.
        bucket_url (str): The url to the bucket.
        filename_filter (str): A glob for the filename filter.

    Returns:
        Iterable[FileItem]: The list of files.
    """
    bucket_url_parsed = urlparse(bucket_url)
    protocol = bucket_url_parsed.scheme or "file"
    bucket_url_str = "".join(bucket_url_parsed[1:3])
    filter_url = "/".join((bucket_url_str.rstrip("/"), filename_filter))

    files = fs_client.glob(filter_url, detail=True)

    for file, md in files.items():
        file_name = file.replace(bucket_url_str, "")
        file_abs_path = "/".join((bucket_url_str.rstrip("/"), file_name))
        file_url = f"{protocol}://{file_abs_path}"
        yield FilesystemFileItem(
            file_name=file_name,
            file_url=file_url,
            mime_type=mimetypes.guess_type(file)[0],
            modification_date=MTIME_DISPATCH[protocol](md),
            file_content=None,
            size_in_bytes=int(md["size"]),
        )
