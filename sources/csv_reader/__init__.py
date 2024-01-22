"""
Source, which uses the `filesystem` source and DuckDB
to extract CSV files data from the given locations.
"""
from typing import Iterable, List

import duckdb
import fsspec
import pendulum

import dlt
from dlt.common.typing import TDataItem, TAnyDateTime
from dlt.common.storages.fsspec_filesystem import prepare_fsspec_args
from dlt.common.storages.configuration import FilesystemConfiguration
from sources.filesystem.helpers import FilesystemConfigurationResource

try:
    from filesystem import filesystem
except ImportError:
    from sources.filesystem import filesystem

from .helpers import add_columns


@dlt.resource(spec=FilesystemConfigurationResource)
def read_location(files, bucket, credentials=dlt.secrets.value):
    """A resource to extract data from the given CSV files.

    Args:
        files (List[FileItem]): A list of files to read.

    Returns:
        Iterable[TDataItem]: Data items, read from the given CSV files.
    """
    config = FilesystemConfiguration(bucket, credentials)
    kwargs = prepare_fsspec_args(config)

    state = dlt.current.resource_state()
    start_from = state.setdefault("last_modified", pendulum.datetime(1970, 1, 1))

    results = []
    connection = duckdb.connect()

    for file in files:
        if file["modification_date"] <= start_from:
            continue

        with fsspec.open(file["file_url"], mode="rb", **kwargs) as f:
            file_res = connection.read_csv(f)
            results += add_columns(file_res.columns, file_res.fetchall())

        state["last_modified"] = max(file["modification_date"], state["last_modified"])

    yield results


@dlt.source
def csv_reader(bucket: str, globs: List[str] = ("*",)) -> Iterable[TDataItem]:
    """
    A source to extract data from CSV files from
    one or several locations.

    Args:
        bucket (str): A bucket URL.
        globs (Optional[List[str]]):
            A list of glob patterns to match files.
            Every glob will be extracted into a separate table.

    Returns:
        Iterable[TDataItem]:
            Data items, read from the matched CSV files.
    """
    for glob in globs:
        files = filesystem(bucket_url=bucket, file_glob=glob)
        yield dlt.resource(read_location(files, bucket))
