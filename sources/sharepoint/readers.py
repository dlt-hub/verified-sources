"""Readers that turn a file of a document library into rows with lineage columns."""

import codecs
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
from dlt.common.pendulum import timedelta
from dlt.common.typing import TDataItems
from dlt.extract import with_table_name
from dlt.sources.filesystem import FileItemDict

TFileType = Literal["csv", "jsonl", "parquet", "excel"]
FILE_TYPES = get_args(TFileType)
BATCH_SIZE = 10_000


def validate_reader_kwargs(file_type: str, reader_kwargs: Dict[str, Any]) -> None:
    """Checks the reader arguments that would fail only after files were downloaded.

    Parquet takes only `columns`, since the other pyarrow arguments change the row shape.

    Raises:
        ValueError: If the file type is unknown, parquet gets anything but `columns`, or jsonl
            gets any argument.
    """
    if file_type not in FILE_TYPES:
        raise ValueError(
            f"file_type must be one of {list(FILE_TYPES)}, got '{file_type}'"
        )
    if file_type == "parquet" and set(reader_kwargs) - {"columns"}:
        raise ValueError(
            f"reader_kwargs for parquet files may only contain 'columns', got {sorted(reader_kwargs)}"
        )
    if file_type == "jsonl" and reader_kwargs:
        raise ValueError(
            f"jsonl files take no reader_kwargs, got {sorted(reader_kwargs)}"
        )


def file_rows(
    items: Iterable[FileItemDict],
    file_type: str,
    reader_kwargs: Dict[str, Any],
    table_name: str,
) -> Iterator[TDataItems]:
    """Yields the rows of each file in batches, with `_file_url`, `_file_name` and `_file_modified_at`.

    The readers mirror dlt's filesystem readers, which are not used because they add no file
    columns, and the file columns are what lets a modified file replace its own rows. Excel
    sheets go to "<table_name>_<sheet>" unless one `sheet_name` is given. A file or sheet with
    no rows yields a deletion marker, so its earlier rows are deleted.
    """
    for item in items:
        lineage = {
            "_file_url": item["file_url"],
            "_file_name": item["file_name"],
            "_file_modified_at": item["modification_date"],
        }
        has_rows: Dict[str, bool] = {}
        with item.open() as f:
            head = f.read(1024)
            blank = len(head) < 1024 and not head.removeprefix(codecs.BOM_UTF8).strip()
            f.seek(0)
            batches = [] if blank else read_records(f, file_type, reader_kwargs)
            for sheet, records in batches:
                table = table_name if sheet is None else f"{table_name}_{sheet}"
                has_rows[table] = has_rows.get(table, False) or bool(records)
                if records:
                    rows = [{**record, **lineage} for record in records]
                    yield with_table_name(rows, table)
        for table in [table_name] if not has_rows else has_rows:
            if not has_rows.get(table):
                yield with_table_name([{**lineage, "_deleted": True}], table)


def read_records(
    f: Any, file_type: str, reader_kwargs: Dict[str, Any]
) -> Iterator[Tuple[Optional[str], List[Dict[str, Any]]]]:
    """Yields batches of records; the first element is the sheet name for a workbook read whole.

    An empty sheet yields one empty batch, so the caller learns the sheet exists.
    """
    if file_type == "parquet":
        parquet_file = pq.ParquetFile(f)
        columns = reader_kwargs.get("columns") or data_column_names(
            parquet_file.schema_arrow
        )
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE, columns=columns):
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
    else:
        sheets = pd.read_excel(f, **{"sheet_name": None, **reader_kwargs})
        for sheet, frame in (
            sheets.items() if isinstance(sheets, dict) else [(None, sheets)]
        ):
            for records in frame_records(frame) if len(frame) else [[]]:
                yield None if sheet is None else str(sheet), records


def frame_records(df: pd.DataFrame) -> Iterator[List[Dict[str, Any]]]:
    """Yields the frame as lists of at most BATCH_SIZE records, with NaN and NaT as None.

    Durations become seconds, header rows are joined with "_" and a named index becomes a column.
    """
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = pd.Index(
            ["_".join(str(level) for level in name) for name in df.columns]
        )
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


def data_column_names(schema: Any) -> List[str]:
    """Returns the column names without the unnamed index columns that pandas writes."""
    index_columns = (schema.pandas_metadata or {}).get("index_columns", [])
    unnamed = {
        name
        for name in index_columns
        if isinstance(name, str) and name.startswith("__index_level_")
    }
    return [name for name in schema.names if name not in unnamed]
