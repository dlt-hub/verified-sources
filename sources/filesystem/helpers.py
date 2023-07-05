import io
import logging
import os
from typing import Any, Dict, Optional, Sequence

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


mime_type_mapper = {".txt": "text/plain", ".pdf": "application/pdf"}


def build_service(
    credentials_path: str, token_path: str = "./token.json"
) -> Optional[Any]:
    credentials = None
    # The file token_path stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        credentials = Credentials.from_authorized_user_file(token_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            credentials = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(token_path, "w", encoding="utf-8") as token:
            token.write(credentials.to_json())

    return build("drive", "v3", credentials=credentials)


def get_files_uris(
    service: Any, folder_id: str, extensions: Sequence[str]
) -> Dict[str, str]:
    files_uris = {}
    for extension in extensions:
        if extension not in mime_type_mapper:
            logging.warning(
                f"Extension {extension} is not implemented, only ({', '.join(mime_type_mapper.keys())}) are available."
            )
            continue
        # Query the Google Drive API to get the files with the specified folder ID and extension
        results = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and mimeType='{mime_type_mapper[extension]}'",
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
