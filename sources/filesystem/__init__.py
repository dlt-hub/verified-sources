import dlt
import os
import logging

from typing import Dict, List, Sequence, Iterable
from pathlib import Path
from dlt.extract.source import DltResource, DltSource, TDataItem
from .helpers import build_service, get_pdf_uris, download_pdf_from_google_drive, check_extensions, ExtensionIsNotImplemented


logging.basicConfig(format="%(asctime)s WARNING: %(message)s", level=logging.WARNING)


@dlt.resource(write_disposition="replace")
def local_folder(
    extensions: Sequence = (".txt", ".pdf"), data_dir: str = dlt.secrets.value,
) -> TDataItem:

    try:
        check_extensions(extensions)
    except ExtensionIsNotImplemented as e:
        logging.warning(f"{e}")

    files = (p.resolve() for p in Path(data_dir).glob("**/*") if p.suffix in set(extensions))
    for file in files:
        if file.is_file():
            yield {"file_path": str(file)}


@dlt.resource(write_disposition="replace")
def google_drive(
    credentials: str = dlt.secrets.value,
    folder_id: str = dlt.secrets.value,
    storage_folder_path: str = dlt.secrets.value,
    download: bool = False
) -> TDataItem:

    service = build_service(credentials)
    uris = get_pdf_uris(service, folder_id)

    for file_name, file_id in uris.items():
        if download:
            download_pdf_from_google_drive(file_id, file_name, storage_folder_path)
            yield {"file_path": os.path.join(storage_folder_path, file_name), "file_name": file_name}
        else:
            yield {"file_name": file_name, "file_id": file_id}

