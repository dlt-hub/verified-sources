"""Converts Table API records, where all values are strings, to typed rows or Arrow tables"""

from datetime import date, datetime, time, timezone  # noqa: I251
from typing import Any, Callable, Dict, List, Set, Tuple

from dlt.common import Decimal
from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.typing import TDataItem

from .query import GLIDE_DATETIME_FORMAT

TRecord = Dict[str, Any]
TConverter = Callable[[str], Any]

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def to_datetime(value: str) -> datetime:
    """Parses a date-time in the database format. Database date-times are in UTC."""
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def to_seconds(value: str) -> int:
    """Parses a duration, stored as a date-time after 1970-01-01 00:00:00, into seconds"""
    return int((to_datetime(value) - EPOCH).total_seconds())


def to_time(value: str) -> time:
    # the instance can store times as date-times on 1970-01-01
    return time.fromisoformat(value[-8:])


def to_bool(value: str) -> bool:
    return value == "true"


CONVERTERS: Dict[str, TConverter] = {
    "bool": to_bool,
    "bigint": int,
    "decimal": Decimal,
    "double": float,
    "timestamp": to_datetime,
    "date": date.fromisoformat,
    "time": to_time,
}


def _converter(column: TColumnSchema, is_duration: bool) -> TConverter:
    if is_duration:
        return to_seconds
    return CONVERTERS.get(column.get("data_type"), str)


def records_to_rows(
    records: List[TRecord], columns: TTableSchemaColumns, durations: Set[str]
) -> List[TDataItem]:
    """Converts `records` into rows with `columns`. Empty values and fields missing from a record,
    for example because a field ACL hides them, are None.

    A value that cannot be converted stays a string. The normalizer then creates a variant column.
    """
    converters: List[Tuple[str, TConverter]] = [
        (name, _converter(column, name in durations))
        for name, column in columns.items()
    ]
    rows: List[TDataItem] = []
    for record in records:
        row: Dict[str, Any] = {}
        for name, convert in converters:
            value = record.get(name)
            if value:
                try:
                    value = convert(value)
                except (ValueError, ArithmeticError):
                    pass
            else:
                value = None
            row[name] = value
        rows.append(row)
    return rows


def records_to_arrow(
    records: List[TRecord], columns: TTableSchemaColumns, durations: Set[str]
) -> Any:
    """Converts `records` into an Arrow table typed as `columns`. Empty values and fields missing
    from a record are nulls.
    """
    # imported here so rows can be converted without pyarrow installed
    from dlt.common.libs.pyarrow import pyarrow as pa

    strings = pa.table(
        {
            name: pa.array([record.get(name) for record in records], pa.string())
            for name in columns
        }
    )
    return strings_to_arrow(strings, columns, durations)


def strings_to_arrow(
    strings: Any, columns: TTableSchemaColumns, durations: Set[str]
) -> Any:
    """Converts an Arrow table with string columns into a table typed as `columns`. Empty strings
    and columns missing from `strings` are nulls.
    """
    from dlt.common.libs.pyarrow import cast_arrow_as_columns_schema, pyarrow as pa
    from pyarrow import compute as pc

    null = pa.scalar(None, pa.string())
    arrays = []
    for name, column in columns.items():
        if name in strings.column_names:
            array = strings.column(name)
            array = pc.if_else(pc.equal(array, ""), null, array)
        else:
            array = pa.nulls(strings.num_rows, pa.string())
        data_type = column.get("data_type")
        if name in durations:
            array = pc.strptime(array, format=GLIDE_DATETIME_FORMAT, unit="s").cast(
                pa.int64()
            )
        elif data_type == "timestamp":
            # parsed without zone, the cast below applies UTC
            array = pc.strptime(array, format=GLIDE_DATETIME_FORMAT, unit="s")
        elif data_type == "time":
            array = pc.utf8_slice_codeunits(array, -8)
        arrays.append(array)
    table = pa.Table.from_arrays(arrays, names=list(columns))
    # arrow types follow the destination capabilities in a pipeline and generic ones elsewhere
    caps = (
        Container().get(DestinationCapabilitiesContext)
        or DestinationCapabilitiesContext.generic_capabilities()
    )
    return cast_arrow_as_columns_schema(table, columns, caps, "UTC")


def unescape_formulas(strings: Any, columns: TTableSchemaColumns) -> Any:
    """Removes the apostrophe that CSV exports put before text that starts with =, +, - or @"""
    from pyarrow import compute as pc

    for name, column in columns.items():
        if column.get("data_type") == "text" and name in strings.column_names:
            unescaped = pc.replace_substring_regex(
                strings.column(name), pattern="^'([=+\\-@])", replacement="\\1"
            )
            strings = strings.set_column(
                strings.column_names.index(name), name, unescaped
            )
    return strings
