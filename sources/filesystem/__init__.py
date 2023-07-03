from pathlib import Path
from typing import Optional, Sequence

import dlt
from dlt.extract.source import TDataItem

from .helpers import build_service, download_pdf_from_google_drive, get_files_uris


@dlt.resource(write_disposition="replace")
def local_folder(
    extensions: Optional[Sequence[str]] = None,
    data_dir: str = dlt.secrets.value,
) -> TDataItem:
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
    service = build_service(credentials_path, token_path)
    uris = get_files_uris(service, folder_id, extensions=extensions)
    storage_folder_path = Path(storage_folder_path)

    if download:
        storage_folder_path.mkdir(exist_ok=True, parents=True)

    for file_name, file_id in uris.items():
        if download:
            file_path = storage_folder_path / file_name

            download_pdf_from_google_drive(service, file_id, file_path.as_posix())

            if file_path.is_file():
                yield {"file_path": file_path.as_posix(), "file_name": file_name}
        else:
            yield {"file_name": file_name, "file_id": file_id}
