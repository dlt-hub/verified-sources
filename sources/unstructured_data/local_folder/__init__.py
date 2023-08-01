"""Those resources collect filepaths from local folder to destinations"""
from pathlib import Path
from typing import Optional, Sequence

import dlt
from dlt.extract.source import TDataItem


@dlt.resource(write_disposition="replace")
def local_folder_source(
    data_dir: str,
    extensions: Optional[Sequence[str]] = None,
) -> TDataItem:
    """
    Retrieves a list of files from a local folder.

    Args:
        data_dir (str): The path to the local folder from which to retrieve the files.
        extensions (Optional[Sequence[str]]): A list of file extensions to filter the files.
            Only files with these extensions will be returned. If None, all files will be returned.
            Defaults to None.

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
            yield {"file_path": file.as_posix(), "file_name": file.name}
