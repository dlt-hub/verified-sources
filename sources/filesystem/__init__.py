"""Reads files in s3, gs or azure buckets using fsspec."""

from typing import Iterator, List, Optional, Union

import dlt
from dlt.sources.filesystem import FileItem, FileItemDict, fsspec_filesystem
from dlt.sources.credentials import FileSystemCredentials

from .helpers import (
    AbstractFileSystem,
    FilesystemConfigurationResource,
    fsspec_from_resource,
    get_files,
)
from .settings import DEFAULT_CHUNK_SIZE


@dlt.resource(
    primary_key="file_url", spec=FilesystemConfigurationResource, standalone=True
)
def filesystem(
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: Optional[str] = "*",
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
) -> Iterator[List[FileItem]]:
    """This source collect files and download or extract data from them.

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials | AbstractFilesystem): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str, optional): The filter to apply to the files in glob format.
        files_per_page (int, optional): The number of files to process at once, defaults to 100.
        extract_content (bool, optional): If true, the content of the file will be extracted if
            false it will return a fsspec file, defaults to False.

    Returns:
        Iterator[List[FileItem]]: The list of files.
    """
    if isinstance(credentials, AbstractFileSystem):
        fs_client = credentials
    else:
        fs_client = fsspec_filesystem(bucket_url, credentials)[0]

    files_chunk: List[FileItem] = []
    for file_model in get_files(fs_client, bucket_url, file_glob):
        file_dict = FileItemDict(file_model, credentials)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)  # type: ignore

        # wait for the chunk to be full
        if len(file_dict) >= files_per_page:
            yield files_chunk
            files_chunk = []
    if files_chunk:
        yield files_chunk
