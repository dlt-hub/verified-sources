"""Readers that turn a file of a document library into rows with file columns."""

import codecs
from datetime import timedelta
from typing import (
    Any,
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

import pandas as pd
import pyarrow.parquet as pq
from dlt.common import json
from dlt.common.typing import TDataItems
from dlt.extract import with_table_name
from dlt.sources.filesystem import FileItemDict

TFileType = Literal["csv", "jsonl", "parquet", "excel"]
FILE_TYPES = get_args(TFileType)
BATCH_SIZE = 10_000


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
    for item in items:
        file_columns = {
            "file_url": item["file_url"],
            "file_name": item["file_name"],
            "file_modified_at": item["modification_date"],
        }
        with item.open() as f:
            head = f.read(1024)
            if len(head) < 1024 and not head.removeprefix(codecs.BOM_UTF8).strip():
                continue
            f.seek(0)
            for sheet, records in read_records(f, file_type, reader_kwargs):
                rows = [{**record, **file_columns} for record in records]
                yield rows if sheet is None else with_table_name(
                    rows, f"{table_name}_{sheet}"
                )


def read_records(
    f: Any, file_type: str, reader_kwargs: Dict[str, Any]
) -> Iterator[Tuple[Optional[str], List[Dict[str, Any]]]]:
    """Yields batches of records; the first element is the sheet name for a workbook read whole."""
    if file_type == "parquet":
        parquet_file = pq.ParquetFile(f)
        for batch in parquet_file.iter_batches(
            batch_size=BATCH_SIZE, columns=reader_kwargs.get("columns")
        ):
            yield None, [
                {name: duration_to_seconds(value) for name, value in record.items()}
                for record in batch.to_pylist()
            ]
    elif file_type == "jsonl":
        lines = (line.removeprefix(codecs.BOM_UTF8) for line in f if line.strip())
        for start_line in lines:
            batch = [json.loadb(start_line)]
            for line in lines:
                batch.append(json.loadb(line))
                if len(batch) >= BATCH_SIZE:
                    break
            yield None, batch
    elif file_type == "csv":
        try:
            reader = pd.read_csv(f, **{"chunksize": BATCH_SIZE, **reader_kwargs})
        except pd.errors.EmptyDataError:
            return
        with reader as chunks:
            for chunk in chunks:
                for records in frame_records(chunk):
                    yield None, records
    elif file_type == "excel":
        sheets = pd.read_excel(f, **{"sheet_name": None, **reader_kwargs})
        for sheet, frame in (
            sheets.items() if isinstance(sheets, dict) else [(None, sheets)]
        ):
            for records in frame_records(frame):
                yield None if sheet is None else str(sheet), records
    else:
        raise ValueError(
            f"file_type must be one of {list(FILE_TYPES)}, got '{file_type}'"
        )


def frame_records(df: pd.DataFrame) -> Iterator[List[Dict[str, Any]]]:
    """Yields the frame as lists of at most BATCH_SIZE records, with NaN and NaT as None.

    Durations become seconds and a named index becomes a column.
    """
    df = df.copy()
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
