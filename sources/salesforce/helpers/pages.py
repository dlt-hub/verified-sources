"""Converts CSV result pages into python rows or Arrow tables typed as dlt columns.

The python backend uses the standard library only. pyarrow is imported just by `csv_page_to_arrow`.
"""

import csv
import io
from typing import Any, Dict, List, Optional

from dlt.common import Decimal
from dlt.common.configuration.container import Container
from dlt.common.data_types.type_helpers import coerce_value
from dlt.common.data_types.typing import TDataType
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.typing import TDataItem


def _coerce(data_type: Optional[TDataType], value: str) -> Any:
    if data_type in (None, "text"):
        return value
    try:
        return coerce_value(data_type, "text", value)
    except ValueError:
        # e.g. `42.0` for bigint, the normalizer creates a variant column for it
        return value


def csv_page_to_rows(page: bytes, columns: TTableSchemaColumns) -> List[TDataItem]:
    """Parses a page of CSV results into dicts with values coerced to `columns` types.

    Blank fields are nulls, `NA` or `NULL` are regular strings in Salesforce.
    """
    reader = csv.reader(io.StringIO(page.decode("utf-8"), newline=""))
    header = next(reader, None)
    if not header:
        return []
    data_types = [columns.get(name, {}).get("data_type") for name in header]
    return [
        {
            name: None if value == "" else _coerce(data_type, value)
            for name, data_type, value in zip(header, data_types, record)
        }
        for record in reader
    ]


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
    from dlt.common.libs.pyarrow import cast_arrow_as_columns_schema, pyarrow as pa
    from pyarrow import compute as pc, csv as pa_csv

    if not page.strip():
        return None
    table = pa_csv.read_csv(
        io.BytesIO(page),
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


def arrow_column_max(table: Any, name: str) -> Any:
    """Returns the max value of the `name` column of an Arrow table"""
    from pyarrow import compute as pc

    return pc.max(table.column(name)).as_py()
