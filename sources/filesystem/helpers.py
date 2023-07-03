import os
import io
import pickle
from typing import Optional, Sequence
from enum import Enum

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


class Extensions(Enum):
    txt = ".txt"
    pdf = ".pdf"


def build_service(creds: Optional = None):
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    elif isinstance(creds, str):
        flow = InstalledAppFlow.from_client_secrets_file(
            creds, SCOPES
        )
        creds = flow.run_local_server(port=0)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def get_pdf_uris(service, folder_id):
    results = service.files().list(
        q=f"'{folder_id}' in parents and mimeType='application/pdf'",
        fields="nextPageToken, files(id, name, mimeType)").execute()
    items = results.get('files', [])
    yield {item['name']: item['id'] for item in items}


def download_pdf_from_google_drive(
    file_id, file_name, storage_folder_path: str = './data/invoices'
):
    service = build_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    print(f"Downloaded {file_name}.")
    with open(os.path.join(storage_folder_path, file_name), 'wb') as f:
        f.write(fh.getvalue())


class ExtensionIsNotImplemented(NotImplementedError):
    pass


def check_extensions(extensions: Sequence[str]) -> None:
    available_exts = [ext.value for ext in Extensions]
    for extension in extensions:
        if extension not in available_exts:
            raise ExtensionIsNotImplemented(
                f"Extension {extension} is not implemented, only ({', '.join(available_exts)}) are available."
            )
