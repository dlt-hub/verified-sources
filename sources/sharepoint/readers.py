"""Readers that turn a file of a document library into rows with file columns."""

import codecs
from datetime import timedelta
from itertools import islice
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    cast,
    get_args,
)

import dlt
import pandas as pd
import pyarrow.parquet as pq
from dlt.common import json
from dlt.common.storages.fsspec_filesystem import FileItem
from dlt.common.typing import TDataItems
from dlt.extract import with_table_name
from dlt.sources.filesystem import FileItemDict

TFileType = Literal["csv", "jsonl", "parquet", "excel"]
FILE_TYPES = get_args(TFileType)
BATCH_SIZE = 10_000
# up to this size a file is downloaded whole on a worker thread, larger ones are read in ranges
PREFETCH_MAX_BYTES = 16 * 2**20

# a batch of records and the workbook sheet it came from, None for every other file type
TSheetRecords = Tuple[Optional[str], List[Dict[str, Any]]]
TReader = Callable[[Any, Dict[str, Any]], Iterator[TSheetRecords]]


@dlt.defer
def read_file(
    item: FileItemDict, file_type: str, reader_kwargs: Dict[str, Any], table_name: str
) -> Iterator[TDataItems]:
    """Downloads the file on a dlt worker thread and returns its rows, which dlt reads in the main thread.

    Files over `PREFETCH_MAX_BYTES` or of unknown size are read in ranges as their rows are read.
    """
    size = item.get("size_in_bytes")
    if size is not None and size <= PREFETCH_MAX_BYTES:
        content = item.read_bytes()
        item = FileItemDict(
            cast(FileItem, {**item, "file_content": content}), item.fsspec
        )
    return file_rows([item], file_type, reader_kwargs, table_name)


def file_rows(
    items: Iterable[FileItemDict],
    file_type: str,
    reader_kwargs: Dict[str, Any],
    table_name: str,
) -> Iterator[TDataItems]:
    """Yields the rows of each file in batches, with `file_url`, `file_name` and `file_modified_at`.

    Rows of the base table carry no table name, so table hints apply to them; Excel sheets go
    to "<table_name>_<sheet>" unless one `sheet_name` is given. Blank files are skipped.
    """
    read = reader_for(file_type)
    for item in items:
        file_columns = {
            "file_url": item["file_url"],
            "file_name": item["file_name"],
            "file_modified_at": item["modification_date"],
        }
        with item.open() as f:
            if is_blank(f):
                continue
            for sheet, records in read(f, reader_kwargs):
                rows = [{**record, **file_columns} for record in records]
                if sheet is None:
                    yield rows
                else:
                    yield with_table_name(rows, f"{table_name}_{sheet}")


def is_blank(f: Any) -> bool:
    """Tells whether the file is empty or holds only whitespace, and rewinds it."""
    head = f.read(1024)
    f.seek(0)
    return len(head) < 1024 and not head.removeprefix(codecs.BOM_UTF8).strip()


def read_csv(f: Any, reader_kwargs: Dict[str, Any]) -> Iterator[TSheetRecords]:
    """Yields batches of records of a csv file, read with pandas."""
    try:
        chunk_reader = pd.read_csv(f, **{"chunksize": BATCH_SIZE, **reader_kwargs})
    except pd.errors.EmptyDataError:
        return
    with chunk_reader as chunks:
        for chunk in chunks:
            for records in frame_records(chunk):
                yield None, records


def read_jsonl(f: Any, reader_kwargs: Dict[str, Any]) -> Iterator[TSheetRecords]:
    """Yields batches of records of a json lines file, skipping blank lines; takes no arguments."""
    records = (
        json.loadb(line.removeprefix(codecs.BOM_UTF8)) for line in f if line.strip()
    )
    while batch := list(islice(records, BATCH_SIZE)):
        yield None, batch


def read_parquet(f: Any, reader_kwargs: Dict[str, Any]) -> Iterator[TSheetRecords]:
    """Yields batches of records of a parquet file; only the "columns" argument is used."""
    batches = pq.ParquetFile(f).iter_batches(
        batch_size=BATCH_SIZE, columns=reader_kwargs.get("columns")
    )
    for batch in batches:
        yield None, [
            {name: duration_to_seconds(value) for name, value in record.items()}
            for record in batch.to_pylist()
        ]


def read_excel(f: Any, reader_kwargs: Dict[str, Any]) -> Iterator[TSheetRecords]:
    """Yields batches of records per workbook sheet, or of the one sheet named in `sheet_name`."""
    sheets = pd.read_excel(f, **{"sheet_name": None, **reader_kwargs})
    for sheet, frame in (
        sheets.items() if isinstance(sheets, dict) else [(None, sheets)]
    ):
        for records in frame_records(frame):
            yield None if sheet is None else str(sheet), records


READERS: Dict[str, TReader] = {
    "csv": read_csv,
    "jsonl": read_jsonl,
    "parquet": read_parquet,
    "excel": read_excel,
}


def reader_for(file_type: str) -> TReader:
    """Returns the reader of the file type."""
    if file_type not in READERS:
        raise ValueError(
            f"file_type must be one of {list(FILE_TYPES)}, got '{file_type}'"
        )
    return READERS[file_type]


def frame_records(df: pd.DataFrame) -> Iterator[List[Dict[str, Any]]]:
    """Yields the frame as lists of at most BATCH_SIZE records, with NaN and NaT as None.

    Durations become seconds, changing the frame in place, and a named index becomes a column.
    """
    if df.index.names != [None]:
        df = df.reset_index()
    for i, dtype in enumerate(df.dtypes):
        if dtype.kind == "m":
            df.isetitem(i, df.iloc[:, i].dt.total_seconds().to_numpy())
        elif dtype == object:
            df.isetitem(i, df.iloc[:, i].map(duration_to_seconds).to_numpy())
    for start in range(0, len(df), BATCH_SIZE):
        part = df.iloc[start : start + BATCH_SIZE]
        yield cast(
            List[Dict[str, Any]],
            part.astype(object).where(part.notna(), None).to_dict(orient="records"),
        )


def duration_to_seconds(value: Any) -> Any:
    """Returns a timedelta as float seconds, which dlt can load, and any other value unchanged."""
    return value.total_seconds() if isinstance(value, timedelta) else value
