"""Converts REST API records to CSV pages and CSV pages to rows or typed Arrow tables"""

import csv
import io
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from dlt.common import Decimal
from dlt.common.configuration.container import Container
from dlt.common.data_types.type_helpers import coerce_value
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.typing import TDataItem

from ..settings import ARROW_BLOCK_SIZE


class TruncatedCsvError(ValueError):
    """A CSV body ended inside a record or before all records it was counted to hold"""


# Apex code exceeds the default 131,072 characters, a larger limit overflows a Windows C long
csv.field_size_limit(2**31 - 1)


def csv_page_to_rows(
    page: bytes, columns: TTableSchemaColumns, cursor: Optional[str] = None
) -> List[TDataItem]:
    """Parses a page of CSV results into dicts of strings, the normalizer coerces them to `columns`.

    Blank fields are nulls, `NA` or `NULL` are regular strings in Salesforce.
    """
    reader = csv.reader(io.StringIO(page.decode("utf-8"), newline=""))
    header = next(reader, None)
    if not header:
        return []
    rows: List[TDataItem] = []
    for record in reader:
        if len(record) != len(header):
            raise ValueError(
                f"A CSV record holds {len(record)} of the {len(header)} fields of the header"
            )
        rows.append(
            {
                name: None if value == "" else value
                for name, value in zip(header, record)
            }
        )
    cursor_type = columns.get(cursor, {}).get("data_type") if cursor else None
    if cursor and cursor_type not in (None, "text"):
        # the incremental compares cursor values before the normalizer coerces them
        for row in rows:
            if row.get(cursor) is not None:
                row[cursor] = coerce_value(cursor_type, "text", row[cursor])
    return rows


def rest_records_to_csv(
    records: List[Dict[str, Any]], columns: TTableSchemaColumns
) -> bytes:
    """Writes REST API query records as a CSV page in the Bulk API format"""
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writerow(columns)
    for record in records:
        writer.writerow(
            [_rest_value_to_csv(record.get(name), columns[name]) for name in columns]
        )
    return buffer.getvalue().encode("utf-8")


def _rest_value_to_csv(value: Any, column: TColumnSchema) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        # JSON numbers are parsed as decimals, written without exponent
        return format(value, "f")
    text = str(value)
    if column.get("data_type") == "timestamp" and text.endswith("+0000"):
        # REST API writes UTC as `+0000`, the Bulk API as `Z`
        return text[:-5] + "Z"
    return text


def csv_page_to_arrow(page: bytes, columns: TTableSchemaColumns) -> Optional[Any]:
    """Parses a page of CSV results into an Arrow table typed as `columns`.

    Returns None for a page without records: a job without results returns just the header.
    """
    # imported here so rows can be parsed without pyarrow installed
    from dlt.common.libs.pyarrow import cast_arrow_as_columns_schema, pyarrow as pa
    from pyarrow import compute as pc, csv as pa_csv

    if not page.strip():
        return None
    table = pa_csv.read_csv(
        io.BytesIO(page),
        read_options=pa_csv.ReadOptions(block_size=ARROW_BLOCK_SIZE),
        parse_options=pa_csv.ParseOptions(newlines_in_values=True),
        convert_options=pa_csv.ConvertOptions(
            column_types={name: pa.string() for name in columns},
            # blank fields are nulls, `NA` or `NULL` are regular strings in Salesforce
            null_values=[""],
            strings_can_be_null=True,
            quoted_strings_can_be_null=True,
        ),
    )
    if table.num_rows == 0:
        return None
    for name in table.column_names:
        if columns.get(name, {}).get("data_type") == "time":
            # times are cast with `time.fromisoformat`, which rejects the `Z` suffix before
            # python 3.11
            table = table.set_column(
                table.schema.get_field_index(name),
                name,
                pc.utf8_rtrim(table.column(name), characters="Z"),
            )
    # generate arrow types compatible with the destination when running in a pipeline
    caps = (
        Container().get(DestinationCapabilitiesContext)
        or DestinationCapabilitiesContext.generic_capabilities()
    )
    return cast_arrow_as_columns_schema(table, columns, caps, "UTC")


def split_csv_records(
    blocks: Iterable[bytes], max_records: int, max_bytes: Optional[int] = None
) -> Iterator[Tuple[bytes, int]]:
    """Splits a streamed Bulk API CSV body into pages of whole records.

    Salesforce quotes every value and doubles the quotes inside values, so a newline ends a
    record only when the record so far holds an even number of quotes. Values may hold newlines.
    Records are counted without parsing their values.

    Args:
        blocks: The CSV body in blocks of any size, e.g. from `Response.iter_content`. Its first
            record is the header.
        max_records: A page ends after this many records.
        max_bytes: A page also ends at the record that brings its records to this many bytes or
            more, the header not counted. None for pages limited by records only.

    Yields:
        Tuple[bytes, int]: A page and its number of records. A page is the header followed by
            one or more whole records, with null bytes removed. A body with only a header yields
            nothing. Records of a body that broke off stay unyielded, the error of `blocks` is
            raised.

    Raises:
        TruncatedCsvError: The body ended inside a record. Salesforce ends every record with a
            newline.
    """
    header: Optional[bytes] = None
    page = bytearray()
    page_records = 0
    pending, scanned, quotes = b"", 0, 0
    for block in blocks:
        data = pending + block
        start, pos = 0, scanned
        while True:
            newline = data.find(b"\n", pos)
            if newline < 0:
                quotes += data.count(b'"', pos)
                break
            quotes += data.count(b'"', pos, newline)
            pos = newline + 1
            if quotes % 2:
                continue
            record = data[start:pos]
            start, quotes = pos, 0
            if header is None:
                header = record
            else:
                page += record
                page_records += 1
                if page_records >= max_records or (
                    max_bytes is not None and len(page) >= max_bytes
                ):
                    # null bytes are not valid in CSV
                    yield header + bytes(page).replace(b"\x00", b""), page_records
                    page.clear()
                    page_records = 0
        pending = data[start:]
        scanned = len(pending)
    if pending.strip() and header is not None:
        raise TruncatedCsvError(
            f"A CSV body ended inside a record, {len(pending)} bytes after its last newline"
        )
    if page_records:
        yield header + bytes(page).replace(b"\x00", b""), page_records


def arrow_column_max(table: Any, name: str) -> Any:
    """Returns the max value of the `name` column of an Arrow table"""
    from pyarrow import compute as pc

    return pc.max(table.column(name)).as_py()
