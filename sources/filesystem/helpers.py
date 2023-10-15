"""Helpers for the filesystem resource."""
from typing import Optional, Type, Union, TYPE_CHECKING, Any
from fsspec import AbstractFileSystem  # type: ignore

from dlt.common.configuration import resolve_type

from dlt.sources import DltResource, DltSource
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
