"""This source collects fsspec files."""

from typing import Iterator, List, Optional

import dlt
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.common.storages.filesystem import FileItem

from .helpers import (
    FilesystemConfigurationResource,
    FileSystemDict,
    client_from_credentials,
    get_files,
)
from .settings import DEFAULT_CHUNK_SIZE


@dlt.resource(
    name="filesystem_r",
    primary_key="file_url",
    selected=False,
    spec=FilesystemConfigurationResource,
)
def filesystem_resource(
    bucket_url: str = dlt.secrets.value,
    credentials: FileSystemCredentials = dlt.secrets.value,
    file_glob: Optional[str] = "*",
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
) -> Iterator[List[FileItem]]:
    """This source collect files and download or extract data from them.

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials): The credentials to the filesystem.
        file_glob (str, optional): The filter to apply to the files in glob format.
        files_per_page (int, optional): The number of files to process at once, defaults to 100.
        extract_content (bool, optional): If true, the content of the file will be extracted if
            false it will return a fsspec file, defaults to False.

    Returns:
        Iterator[List[FileItem]]: The list of files.
    """

    fs_client = client_from_credentials(bucket_url, credentials)

    files_chunk: List[FileItem] = []
    for file_model in get_files(fs_client, bucket_url, file_glob):
        file_dict = FileSystemDict(file_model, credentials)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)  # type: ignore

        # wait for the chunk to be full
        if len(file_dict) >= files_per_page:
            yield files_chunk
            files_chunk = []
    if files_chunk:
        yield files_chunk
