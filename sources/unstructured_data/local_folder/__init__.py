"""Those resources collect filepaths from local folder to destinations"""
from pathlib import Path
from typing import Sequence, Union

import dlt
from dlt.extract.source import TDataItem

MIME_TYPE_MAPPER = {
    ".txt": "text/plain",
    ".pdf": "application/pdf",
    ".ics": "application/ics",
}


@dlt.resource(write_disposition="replace")
def local_folder_source(
    data_dir: Union[str, Sequence[str]],
) -> TDataItem:
    """
    Retrieves a list of files from a local folder.

    Args:
        data_dir (str): The path to the local folder from which to retrieve the files.

    Yields:
        TDataItem: A dictionary representing a file, containing the file path.
    """
    if isinstance(data_dir, (list, tuple)):
        for folder in data_dir:
            yield get_files(folder)
    elif isinstance(data_dir, str):
        yield get_files(data_dir)


def get_files(data_dir: str) -> TDataItem:
    if not Path(data_dir).is_dir():
        if not Path(data_dir).exists():
            raise ValueError(f"Local folder doesn't exist: {data_dir}")
        else:
            raise ValueError(f"Local folder is not a directory: {data_dir}")

    files = (p.resolve() for p in Path(data_dir).glob("**/*"))

    for file in files:
        if file.is_file():
            yield {
                "file_path": file.as_posix(),
                "file_name": file.name,
                "content_type": MIME_TYPE_MAPPER.get(file.suffix, "other"),
            }
