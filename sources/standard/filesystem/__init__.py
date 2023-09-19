"""This source collects fsspec files."""
import os
from typing import Any, Dict, Optional

import dlt
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.extract.source import TDataItems

from .helpers import client_from_credentials, get_files
from .settings import DEFAULT_CHUNK_SIZE


@dlt.resource(
    name="filesystem",
    merge_key="file_url",
    primary_key="file_url",
    spec=FilesystemConfiguration,
)
def filesystem_resource(
    credentials: FilesystemConfiguration = dlt.secrets.value,
    filename_filter: Optional[str] = None,
    chunksize: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
) -> TDataItems:
    """This source collect files and download or extract data from them.

    Args:
        credentials (FilesystemConfiguration): The credentials to the filesystem.
        filename_filter (str, optional): The filter to apply to the files in glob format.
        chunksize (int, optional): The number of files to process at once, defaults to 10.
        extract_content (bool, optional): If true, the content of the file will be extracted if
            false it will return a fsspec file, defaults to False.

    Returns:
        TDataItems: The list of files.
    """

    fs_client = client_from_credentials(credentials)

    if not filename_filter:
        filename_filter = "*"
    files_url = os.path.join(credentials.bucket_url, filename_filter)

    files_chunk = []

    for file_model in get_files(fs_client, files_url):
        file_dict: Dict[str, Any] = dict(**file_model)
        if extract_content:
            file_dict["content"] = fs_client.read_bytes(file_dict["file_url"])
        # Need to check if passing an open file is ok, probably we will need to create a class
        # to manage the context, closing the file after the process.
        else:
            file_dict["file_instance"] = fs_client.open(file_dict["file_url"])
        files_chunk.append(file_dict)
        if len(file_dict) >= chunksize:
            yield files_chunk
            files_chunk = []
