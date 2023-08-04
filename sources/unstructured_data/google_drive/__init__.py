"""This resource downloads files and collects filepaths from Google Drive folder to destinations"""
from copy import deepcopy
from pathlib import Path
from typing import Any, Sequence, Union

import dlt
from dlt.common import logger
from dlt.extract.source import TDataItem, TDataItems
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials
from googleapiclient.discovery import build  # type: ignore

from .helpers import download_file_from_google_drive
from .settings import FOLDER_IDS, STORAGE_FOLDER_PATH

SCOPE = "https://www.googleapis.com/auth/drive.readonly"


@dlt.source(name="google_drive")
def google_drive_source(
    credentials: Union[
        GcpOAuthCredentials, GcpServiceAccountCredentials
    ] = dlt.secrets.value,
    folder_ids: Sequence[str] = FOLDER_IDS,
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    download: bool = False,
    filter_by_mime_type: Sequence[str] = (),
) -> TDataItem:
    """
    Retrieves files from specified Google Drive folders.

    Args:
        credentials (Dict[str, Any]): The authorized user info in Google format for authenticating with Google Drive API.
            Defaults to dlt.secrets.value.
        folder_ids (Sequence[str]): A sequence of folder IDs from which to retrieve the files.
            Defaults to FOLDER_IDS (see settings.py).
        storage_folder_path (str): The path to the local folder where the downloaded files will be stored.
            Defaults to STORAGE_FOLDER_PATH (see settings.py).
        download (bool): Indicates whether to download the files or not. If True, the files will be downloaded and stored locally.
            If False, only the file names and IDs will be yielded without downloading.
            Defaults to False.
        filter_by_mime_type (Sequence[str], optional): A sequence of MIME types used to filter attachments based on their content type. Default is an empty sequence.

    Yields:
        TDataItem: A dictionary representing a file.

    """
    # check if credentials are provided
    if isinstance(credentials, GcpOAuthCredentials):
        credentials.auth(SCOPE)

    service = build("drive", "v3", credentials=credentials.to_native_credentials())

    if download:
        storage_folder_path_ = Path(storage_folder_path)
        storage_folder_path_.mkdir(exist_ok=True, parents=True)

        yield get_files_uris(service, folder_ids) | download_files(
            service, storage_folder_path_, filter_by_mime_type
        )
    else:
        yield get_files_uris(service, folder_ids)


@dlt.resource(name="files_ids")
def get_files_uris(service: Any, folder_ids: Sequence[str]) -> TDataItems:
    # Query the Google Drive API to get the files with the specified folder ID and extension
    for folder_id in folder_ids:
        results = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents",
                fields="nextPageToken, files",
            )
            .execute()
        )

        items = results.get("files", [])

        if not items:
            logger.warning(f"No files found in directory {folder_id}!")
        else:
            yield [convert_response_to_standard(item) for item in items]


def convert_response_to_standard(response: TDataItem) -> TDataItem:
    return {
        "content_type": response["mimeType"],
        "parents": response["parents"],
        "file_id": response["id"],
        "file_name": response["name"],
        "modification_date": response["modifiedTime"],
        "date": response["createdTime"],
    }


@dlt.transformer(name="attachments")
def download_files(
    items: TDataItems,
    service: Any,
    storage_folder_path: Path,
    filter_by_mime_type: Sequence[str] = (),
) -> TDataItem:
    for item in items:
        if filter_by_mime_type and item["content_type"] not in filter_by_mime_type:
            continue

        file_name, file_id = item["file_name"], item["file_id"]
        result = deepcopy(item)

        file_path = storage_folder_path / file_name
        download_file_from_google_drive(service, file_id, file_path.as_posix())
        if file_path.is_file():
            result["file_path"] = file_path.absolute().as_posix()

        yield result
