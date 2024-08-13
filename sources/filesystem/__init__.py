from typing import Optional, Union, List

import dlt
from dlt.sources.credentials import FileSystemCredentials
from dlt.sources.filesystem import FileItem, FileItemDict

from .helpers import (
    AbstractFileSystem,
    FilesystemConfigurationResource,
)
from .readers import (
    ReadersSource,
    _read_csv,
    _read_csv_duckdb,
    _read_jsonl,
    _read_json,
    _read_parquet,
)
from .filesystem_resource import filesystem_resource


reader_map = {
    "parquet": _read_parquet,
    "csv": _read_csv,
    "csv_duckdb": _read_csv_duckdb,
    "jsonl": _read_jsonl,
    "json": _read_json,
}


@dlt.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def filesystem(
    file_glob: str,
    table_names: List[str],
    database_name: str = None,
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_format: Optional[str] = "parquet",
    file_compression: Optional[str] = "",
    step: str = "load",
):
    for table_name in table_names:
        if step == "load":
            file_glob = f"{database_name}/{table_name}/*.{file_format}"

    for table_name in table_names:
        reader = reader_map.get(file_format)
        yield filesystem_resource(
            bucket_url,
            credentials,
            file_glob=file_glob,
            file_compression=file_compression,
        ) | dlt.transformer(name=table_name)(reader)


__all__ = ["FileDict", "FileDictItems", "filesystem"]
