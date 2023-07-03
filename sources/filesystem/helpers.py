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

    if os.path.exists(token_path):
        credentials = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            credentials = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(token_path, "w") as token:
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


def download_pdf_from_google_drive(service: Any, file_id: str, file_path: str) -> None:
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        with open(file_path, "wb") as f:
            f.write(fh.getvalue())

    except HttpError as error:
        print(f"An error occurred: {error}")
