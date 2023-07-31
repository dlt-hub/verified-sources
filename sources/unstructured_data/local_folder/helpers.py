import io
import logging
import os
from typing import Any, Dict, Optional, Sequence

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from .settings import MIME_TYPE_MAPPER, SCOPES

logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)


def build_service(
    client_secret_path: Optional[str] = None,
    token_path: Optional[str] = "./token.json",
    user_info: Optional[Dict[str, Any]] = None,
) -> Optional[Any]:
    """
    Builds a Google Drive API service object for interacting with Google Drive.

    Args:
        client_secret_path (Optional[str]): The path to the client secret JSON file for authentication. Defaults to None.
        token_path (Optional[str]): The path to the token JSON file for storing and reusing authentication tokens. Defaults to "./token.json".
        user_info (Optional[Dict[str, Any]]): User information dictionary containing access and refresh tokens. Defaults to None.

    Returns:
        Optional[Any]: A service object for interacting with the Google Drive API.

    Note:
        - If `user_info` is provided, the function creates `Credentials` object from the authorized user info in Google format.
        - If `user_info` is not provided but `token_path` exists, the function creates `Credentials` object from the authorized user json file.
        - If no valid credentials are available, the function initiates the authorization flow using the client secret file.
        - If `token_path` is provided, the obtained or refreshed credentials are saved to the token file.
    """
    credentials = None

    if user_info:
        credentials = Credentials.from_authorized_user_info(user_info, SCOPES)
    elif os.path.exists(token_path):
        credentials = Credentials.from_authorized_user_file(token_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        try:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
        except RefreshError as error:
            if os.path.exists(client_secret_path):
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secret_path, SCOPES
                )
                credentials = flow.run_local_server(port=0)
            else:
                raise error

        if token_path:
            # Save the credentials for the next run
            with open(token_path, "w", encoding="utf-8") as token:
                token.write(credentials.to_json())

    return build("drive", "v3", credentials=credentials)


def get_files_uris(
    service: Any, folder_id: str, extensions: Sequence[str]
) -> Dict[str, str]:
    files_uris = {}
    for extension in extensions:
        if extension not in MIME_TYPE_MAPPER:
            logging.warning(
                f"Extension {extension} is not implemented, only ({', '.join(MIME_TYPE_MAPPER.keys())}) are available."
            )
            continue
        # Query the Google Drive API to get the files with the specified folder ID and extension
        results = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and mimeType='{MIME_TYPE_MAPPER[extension]}'",
                fields="nextPageToken, files(id, name, mimeType)",
            )
            .execute()
        )

        items = results.get("files", [])
        files_uris.update({item["name"]: item["id"] for item in items})

    return files_uris


def download_file_from_google_drive(service: Any, file_id: str, file_path: str) -> None:
    try:
        # Create a request to download the file
        request = service.files().get_media(fileId=file_id)
        # Create a BytesIO object to store the file content
        fh = io.BytesIO()
        # Create a downloader object to handle the download
        downloader = MediaIoBaseDownload(fh, request)
        # Flag to track if the download is complete
        done = False
        # Download the next chunk of data and check if the download is complete
        while not done:
            _, done = downloader.next_chunk()
        # Write the downloaded file content to the specified file path
        with open(file_path, "wb") as f:
            f.write(fh.getvalue())

    except HttpError as error:
        print(f"An error occurred: {error}")
