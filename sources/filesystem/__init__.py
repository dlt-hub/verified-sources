"""Those resources collect filepaths from local folder or Google Drive folder to destinations"""

from pathlib import Path
from typing import Optional, Sequence

import dlt
from dlt.extract.source import TDataItem

from .helpers import build_service, download_file_from_google_drive, get_files_uris


@dlt.resource(write_disposition="replace")
def local_folder(
    extensions: Optional[Sequence[str]] = None,
    data_dir: str = dlt.secrets.value,
) -> TDataItem:
    """
    Retrieves a list of files from a local folder.

    Args:
        extensions (Optional[Sequence[str]]): A list of file extensions to filter the files.
            Only files with these extensions will be returned. If None, all files will be returned.
            Defaults to None.
        data_dir (str): The path to the local folder from which to retrieve the files.
            Defaults to the value of `dlt.secrets.value`.

    Yields:
        TDataItem: A dictionary representing a file, containing the file path.
    """
    if extensions:
        files = (
            p.resolve()
            for p in Path(data_dir).glob("**/*")
            if p.suffix in set(extensions)
        )
    else:
        files = (p.resolve() for p in Path(data_dir).glob("**/*"))

    for file in files:
        if file.is_file():
            yield {"file_path": file.as_posix()}


@dlt.resource(write_disposition="replace")
def google_drive(
    extensions: Sequence[str] = (".txt", ".pdf"),
    credentials_path: str = dlt.secrets.value,
    token_path: str = dlt.secrets.value,
    folder_id: str = dlt.secrets.value,
    storage_folder_path: str = dlt.secrets.value,
    download: bool = False,
) -> TDataItem:
    """
    Retrieves files from a specified Google Drive folder.

    Args:
        extensions (Sequence[str]): A sequence of file extensions to filter the files.
            Only files with these extensions will be retrieved.
            Defaults to (".txt", ".pdf").
        credentials_path (str): The path to the credentials JSON file for authenticating with Google Drive API.
            Defaults to the value of `dlt.secrets.value`.
        token_path (str): The path to the token JSON file for storing and reusing the authentication token.
            Defaults to the value of `dlt.secrets.value`.
        folder_id (str): The ID of the Google Drive folder from which to retrieve the files.
            Defaults to the value of `dlt.secrets.value`.
        storage_folder_path (str): The path to the local folder where the downloaded files will be stored.
            Defaults to the value of `dlt.secrets.value`.
        download (bool): Indicates whether to download the files or not.
            If True, the files will be downloaded and stored locally.
            If False, only the file names and IDs will be yielded without downloading.
            Defaults to False.

    Yields:
        TDataItem: A dictionary representing a file. If download is True, the dictionary contains the following keys:
                - "file_path" (str): The local path of the downloaded file.
                - "file_name" (str): The name of the file.
                - "file_id" (str): The ID of the file in Google Drive.
                - "folder_id" (str): The ID of the Google Drive folder.
            If download is False, the dictionary contains the following keys:
                - "file_name" (str): The name of the file.
                - "file_id" (str): The ID of the file in Google Drive.
                - "folder_id" (str): The ID of the Google Drive folder.
    """
    # create drive api client
    service = build_service(credentials_path, token_path)
    uris = get_files_uris(service, folder_id, extensions=extensions)
    storage_folder_path_ = Path(storage_folder_path)

    if download:
        storage_folder_path_.mkdir(exist_ok=True, parents=True)

    for file_name, file_id in uris.items():
        if download:
            file_path = storage_folder_path_ / file_name

            download_file_from_google_drive(service, file_id, file_path.as_posix())

            if file_path.is_file():
                yield {
                    "file_path": file_path.absolute().as_posix(),
                    "file_name": file_name,
                    "file_id": file_id,
                    "folder_id": folder_id
                }
        else:
            yield {"file_name": file_name, "file_id": file_id, "folder_id": folder_id}
