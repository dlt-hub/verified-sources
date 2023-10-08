"""Helpers for the filesystem resource."""
import mimetypes
import posixpath
from typing import Iterable, Optional, Type, Union
from urllib.parse import urlparse
from fsspec import AbstractFileSystem  # type: ignore

from dlt.common.configuration import resolve_type
from dlt.common.storages.fsspec_filesystem import MTIME_DISPATCH, FileItem

from dlt.sources import DltResource
from dlt.sources.filesystem import fsspec_filesystem
from dlt.sources.config import configspec, with_config
from dlt.sources.credentials import (
    CredentialsConfiguration,
    FilesystemConfiguration,
    FileSystemCredentials,
)

from .settings import DEFAULT_CHUNK_SIZE


@configspec
class FilesystemConfigurationResource(FilesystemConfiguration):
    credentials: Union[FileSystemCredentials, AbstractFileSystem]
    file_glob: Optional[str] = "*"
    files_per_page: int = DEFAULT_CHUNK_SIZE
    extract_content: bool = False

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return Union[self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration], AbstractFileSystem]  # type: ignore[return-value]


def fsspec_from_resource(filesystem_instance: DltResource) -> AbstractFileSystem:
    """Extract authorized fsspec client from a filesystem resource"""

    @with_config(
        spec=FilesystemConfiguration,
        sections=("sources", filesystem_instance.section, filesystem_instance.name),
    )
    def _get_fsspec(
        bucket_url: str, credentials: FileSystemCredentials
    ) -> AbstractFileSystem:
        print(bucket_url)
        return fsspec_filesystem(bucket_url, credentials)[0]

    return _get_fsspec(
        filesystem_instance.explicit_args.get("bucket_url", None),
        filesystem_instance.explicit_args.get("credentials", None),
    )


def get_files(
    fs_client: AbstractFileSystem, bucket_url: str, file_glob: str = "**/*"
) -> Iterable[FileItem]:
    """Get the files from the filesystem client.

    Args:
        fs_client (AbstractFileSystem): The filesystem client.
        bucket_url (str): The url to the bucket.
        file_glob (str): A glob for the filename filter.

    Returns:
        Iterable[FileItem]: The list of files.
    """
    bucket_url_parsed = urlparse(bucket_url)
    protocol = bucket_url_parsed.scheme or "file"
    # absolute paths are not joined, so we remove it from the bucket_url path
    bucket_url_str = posixpath.join(
        bucket_url_parsed.netloc, bucket_url_parsed.path.lstrip("/")
    )
    filter_url = posixpath.join(bucket_url_str, file_glob)

    if bucket_url.startswith("file:///"):
        filter_url = posixpath.join("/", filter_url)

    glob_result = fs_client.glob(filter_url, detail=True)

    for file, md in glob_result.items():
        if md["type"] != "file":
            continue
        file_name = file.replace(bucket_url_str, "").lstrip("/")
        file_url = f"{protocol}://{file}"
        yield FileItem(
            file_name=file_name,
            file_url=file_url,
            mime_type=mimetypes.guess_type(file)[0],
            modification_date=MTIME_DISPATCH[protocol](md),
            file_content=None,
            size_in_bytes=int(md["size"]),
        )
