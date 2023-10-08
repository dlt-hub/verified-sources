"""Helpers for the filesystem resource."""
import mimetypes
import posixpath
import io
from io import BytesIO, IOBase
from typing import Iterable, Optional, Type, Dict, Any, Union
from urllib.parse import urlparse
from fsspec import AbstractFileSystem  # type: ignore

from dlt.common.configuration import resolve_type
from dlt.common.storages import filesystem
from dlt.common.storages.filesystem import MTIME_DISPATCH, FileItem

from dlt.sources import DltResource
from dlt.sources.config import configspec, with_config
from dlt.sources.credentials import CredentialsConfiguration, FilesystemConfiguration, FileSystemCredentials

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


class FileSystemDict(Dict[str, Any]):
    """A dictionary with the filesystem client.

    This class represents a dictionary that is backed by a filesystem. The keys and values
    of the dictionary are stored in a file on the filesystem.
    """

    def __init__(
        self, mapping: FileItem, credentials: Optional[Union[FileSystemCredentials, AbstractFileSystem]] = None
    ):
        """Create a dictionary with the filesystem client.

        Args:
            mapping (FileItem): The file item TypedDict.
            credentials (Optional[FileSystemCredentials], optional): The credentials to the
                filesystem. Defaults to None.
        """
        self.credentials = credentials
        super().__init__(**mapping)

    @property
    def filesystem(self) -> AbstractFileSystem:
        """The filesystem client based on the given credentials.

        Returns:
            AbstractFileSystem: The filesystem client.
        """
        if isinstance(self.credentials, AbstractFileSystem):
            return self.credentials
        else:
            return fsspec_from_credentials(self["file_url"], self.credentials)

    def open(self, **kwargs: Any) -> IOBase:  # noqa: A003
        """Open the file as a fsspec file.

        This method opens the file represented by this dictionary as a file-like object using
        the fsspec library.

        Args:
            **kwargs (Any): The arguments to pass to the fsspec open function.

        Returns:
            IOBase: The fsspec file.
        """
        opened_file: IOBase
        # if the user has already extracted the content, we use it so there will be no need to
        # download the file again.
        if self["file_content"] in self:
            bytes_io = BytesIO(self["file_content"])

            text_kwargs = {
                k: kwargs.pop(k)
                for k in ["encoding", "errors", "newline"]
                if k in kwargs
            }
            return io.TextIOWrapper(
                bytes_io,
                **text_kwargs,
            )
        else:
            opened_file = self.filesystem.open(self["file_url"], **kwargs)
        return opened_file

    def read_bytes(self) -> bytes:
        """Read the file content.

        Returns:
            bytes: The file content.
        """
        content: bytes
        # same as open, if the user has already extracted the content, we use it.
        if "file_content" in self and self["file_content"] is not None:
            content = self["file_content"]
        else:
            content = self.filesystem.read_bytes(self["file_url"])
        return content


def fsspec_from_credentials(
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


def fsspec_from_resource(filesystem_instance: DltResource) -> AbstractFileSystem:
    """Extract authorized fsspec client from a filesystem resource"""

    @with_config(spec=FilesystemConfiguration, sections=("sources", filesystem_instance.section, filesystem_instance.name))
    def _get_fsspec(bucket_url: str, credentials: FileSystemCredentials) -> AbstractFileSystem:
        print(bucket_url)
        return fsspec_from_credentials(bucket_url, credentials)

    return _get_fsspec(filesystem_instance.explicit_args.get("bucket_url", None), filesystem_instance.explicit_args.get("credentials", None))


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
