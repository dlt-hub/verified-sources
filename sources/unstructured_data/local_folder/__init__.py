"""Those resources collect filepaths from local folder or Google Drive folder to destinations"""
import logging
from pathlib import Path
from typing import Optional, Sequence, Dict, Any

import dlt
from dlt.extract.source import TDataItem

from .helpers import build_service, download_file_from_google_drive, get_files_uris
from .settings import (
    AUTHORIZED_USER_PATH,
    DATA_DIR,
    FOLDER_IDS,
    STORAGE_FOLDER_PATH,
    ClIENT_SECRET_PATH,
)


@dlt.resource(write_disposition="replace")
def local_folder(
    extensions: Optional[Sequence[str]] = None,
    data_dir: str = DATA_DIR,
) -> TDataItem:
    """
    Retrieves a list of files from a local folder.

    Args:
        extensions (Optional[Sequence[str]]): A list of file extensions to filter the files.
            Only files with these extensions will be returned. If None, all files will be returned.
            Defaults to None.
        data_dir (str): The path to the local folder from which to retrieve the files.
            Defaults to DATA_DIR (see settings.py).

    Yields:
        TDataItem: A dictionary representing a file, containing the file path.
    """
    if not Path(data_dir).is_dir():
        if not Path(data_dir).exists():
            raise ValueError(f"Local folder doesn't exist: {data_dir}")
        else:
            raise ValueError(f"Local folder is not a directory: {data_dir}")

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
