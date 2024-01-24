from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import dlt
from dlt.common import json
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import prepare_fsspec_args
from dlt.common.typing import copy_sig
from dlt.sources import TDataItems, DltResource, DltSource
from dlt.sources.filesystem import FileItemDict

try:
    from filesystem.helpers import (
        AbstractFileSystem,
        FileSystemCredentials,
    )
except ImportError:
    from sources.filesystem.helpers import (
        AbstractFileSystem,
        FileSystemCredentials,
    )


def _read_csv(
    items: Iterator[FileItemDict], chunksize: int = 10000, **pandas_kwargs: Any
) -> Iterator[TDataItems]:
    """Reads csv file with Pandas chunk by chunk.

    Args:
        chunksize (int): Number of records to read in one chunk
        **pandas_kwargs: Additional keyword arguments passed to Pandas.read_csv
    Returns:
        TDataItem: The file content
    """
    import pandas as pd

    # apply defaults to pandas kwargs
    kwargs = {**{"header": "infer", "chunksize": chunksize}, **pandas_kwargs}

    for file_obj in items:
        # Here we use pandas chunksize to read the file in chunks and avoid loading the whole file
        # in memory.
        with file_obj.open() as file:
            for df in pd.read_csv(file, **kwargs):
                yield df.to_dict(orient="records")


def _read_jsonl(
    items: Iterator[FileItemDict], chunksize: int = 1000
) -> Iterator[TDataItems]:
    """Reads jsonl file content and extract the data.

    Args:
        chunksize (int, optional): The number of JSON lines to load and yield at once, defaults to 1000

    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        with file_obj.open() as f:
            lines_chunk = []
            for line in f:
                lines_chunk.append(json.loadb(line))
                if len(lines_chunk) >= chunksize:
                    yield lines_chunk
                    lines_chunk = []
        if lines_chunk:
            yield lines_chunk


def _read_parquet(
    items: Iterator[FileItemDict],
    chunksize: int = 10,
) -> Iterator[TDataItems]:
    """Reads parquet file content and extract the data.

    Args:
        chunksize (int, optional): The number of files to process at once, defaults to 10.

    Returns:
        TDataItem: The file content
    """
    from pyarrow import parquet as pq

    for file_obj in items:
        with file_obj.open() as f:
            parquet_file = pq.ParquetFile(f)
            for rows in parquet_file.iter_batches(batch_size=chunksize):
                yield rows.to_pylist()


def _add_columns(columns: List[str], rows: List[List[Any]]):
    """Adds column names to the given rows.

    Args:
        columns (List[str]): The column names.
        rows (List[List[Any]]): The rows.

    Returns:
        List[Dict[str, Any]]: The rows with column names.
    """
    result = []
    for row in rows:
        result.append(dict(zip(columns, row)))

    return result


def _fetch_arrow(file_data, chunk_size):
    """Fetches data from the given CSV file.

    Args:
        file_data (DuckDBPyRelation): The CSV file data.
        chunk_size (int): The number of rows to read at once.

    Yields:
        Iterable[TDataItem]: Data items, read from the given CSV file.
    """
    batcher = file_data.fetch_arrow_reader(batch_size=chunk_size)
    yield from batcher


def _fetch_json(file_data, chunk_size):
    """Fetches data from the given CSV file.

    Args:
        file_data (DuckDBPyRelation): The CSV file data.
        chunk_size (int): The number of rows to read at once.

    Yields:
        Iterable[TDataItem]: Data items, read from the given CSV file.
    """
    batch = True
    while batch:
        batch = file_data.fetchmany(chunk_size)
        yield _add_columns(file_data.columns, batch)


def _read_csv_duckdb(
    items: Iterator[FileItemDict],
    bucket: str,
    chunk_size: Optional[int] = 5000,
    use_pyarrow: bool = False,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    read_csv_kwargs: Optional[Dict] = {},
):
    """A resource to extract data from the given CSV files.

    Uses DuckDB engine to import and cast CSV data.

    Args:
        items (Iterator[FileItemDict]): CSV files to read.
        bucket (str): The bucket name.
        chunk_size (Optional[int]):
            The number of rows to read at once. Defaults to 5000.
        use_pyarrow (bool):
            Whether to use `pyarrow` to read the data and designate
            data schema. If set to False (by default), JSON is used.
        credentials (Union[FileSystemCredentials, AbstractFileSystem]):
            The credentials to use. Defaults to dlt.secrets.value.
        read_csv_kwargs (Optional[Dict]):
            Additional keyword arguments passed to `read_csv()`.

    Returns:
        Iterable[TDataItem]: Data items, read from the given CSV files.
    """
    import duckdb
    import fsspec
    import pendulum

    config = FilesystemConfiguration(bucket, credentials)
    fs_kwargs = prepare_fsspec_args(config)

    state = dlt.current.resource_state()
    start_from = state.setdefault("last_modified", pendulum.datetime(1970, 1, 1))

    connection = duckdb.connect()

    helper = _fetch_arrow if use_pyarrow else _fetch_json

    for item in items:
        if item["modification_date"] <= start_from:
            continue

        with fsspec.open(item["file_url"], mode="rb", **fs_kwargs) as f:
            file_data = connection.read_csv(f, **read_csv_kwargs)

            yield from helper(file_data, chunk_size)

        state["last_modified"] = max(item["modification_date"], state["last_modified"])


if TYPE_CHECKING:

    class ReadersSource(DltSource):
        """This is a typing stub that provides docstrings and signatures to the resources in `readers" source"""

        @copy_sig(_read_csv)
        def read_csv(self) -> DltResource:
            ...

        @copy_sig(_read_jsonl)
        def read_jsonl(self) -> DltResource:
            ...

        @copy_sig(_read_parquet)
        def read_parquet(self) -> DltResource:
            ...

        @copy_sig(_read_csv_duckdb)
        def read_location(self) -> DltResource:
            ...

else:
    ReadersSource = DltSource
