import json
from datetime import date, datetime, time, timedelta, timezone
from functools import partial
from typing import Any, Callable, Dict, List, NamedTuple, Optional

from dlt.common import Decimal, logger
from dlt.common.data_types.type_helpers import coerce_value
from dlt.common.data_types.typing import TDataType
from dlt.common.schema.typing import TColumnSchema, TColumnType

from .decoders import ColumnType

TValueConverter = Callable[[str], Any]

SESSION_OPTIONS = (
    "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3 -c timezone=UTC"
    " -c bytea_output=hex -c client_encoding=UTF8"
)
"""Pins settings that format values as text, same as Postgres' own subscriber."""


def add_session_options(options: Optional[str]) -> str:
    """Appends `SESSION_OPTIONS` to libpq `options`, so they override the user's settings."""
    return f"{options} {SESSION_OPTIONS}" if options else SESSION_OPTIONS


_DUMMY_VALS: Dict[TDataType, Any] = {
    "bigint": 0,
    "binary": b" ",
    "bool": True,
    "json": [0],
    "date": date(2000, 1, 1),
    "decimal": Decimal(0),
    "double": 0.0,
    "text": "",
    "time": time(0, 0, 0),
    "timestamp": datetime(2000, 1, 1, tzinfo=timezone.utc),
    "wei": 0,
}
"""Dummy values used to replace NULLs in NOT NULL columns in key-only delete records."""

_ARRAY_ELEMENT_TYPES: Dict[int, int] = {
    1000: 16,
    1001: 17,
    1005: 21,
    1007: 23,
    1016: 20,
    1021: 700,
    1022: 701,
    1231: 1700,
    1009: 25,
    1015: 1043,
    1014: 1042,
    1182: 1082,
    1183: 1083,
    1270: 1266,
    1115: 1114,
    1185: 1184,
    1187: 1186,
    199: 114,
    3807: 3802,
    2951: 2950,
}
"""Maps built-in array type OIDs to their element type OIDs."""

_KNOWN_TYPES = {
    16,
    17,
    18,
    19,
    20,
    21,
    23,
    25,
    26,
    114,
    650,
    700,
    701,
    790,
    829,
    869,
    1042,
}
_KNOWN_TYPES |= {1043, 1082, 1083, 1114, 1184, 1186, 1266, 1700, 2950, 3802}
"""Built-in type OIDs that need no catalog lookup."""


class PgType(NamedTuple):
    """Resolved type of a column: domains are replaced by their base type."""

    type_id: int
    atttypmod: int
    element_type_id: Optional[int] = None


def resolve_builtin_type(type_id: int, atttypmod: int) -> Optional[PgType]:
    """Returns type for well known built-in types, None if the catalog must be queried."""
    if type_id in _ARRAY_ELEMENT_TYPES:
        return PgType(type_id, atttypmod, _ARRAY_ELEMENT_TYPES[type_id])
    if type_id in _KNOWN_TYPES:
        return PgType(type_id, atttypmod)
    return None


def to_dlt_column_type(pg_type: PgType) -> TColumnType:
    """Maps a Postgres type to a dlt column type.

    Follows `sql_table` reflection with `full_with_precision`, so snapshot and replicated
    tables get the same columns. Unknown types (enums, network types, intervals...) map to text.
    """
    type_id, atttypmod = pg_type.type_id, pg_type.atttypmod
    if pg_type.element_type_id is not None or type_id in (114, 3802):
        return {"data_type": "json"}
    if type_id == 16:
        return {"data_type": "bool"}
    if type_id == 17:
        return {"data_type": "binary"}
    if type_id == 21:
        return {"data_type": "bigint", "precision": 32}
    if type_id in (20, 23, 26):
        return {"data_type": "bigint"}
    if type_id in (700, 701):
        return {"data_type": "double"}
    if type_id == 1700:
        if atttypmod == -1:
            return {"data_type": "decimal"}
        return {
            "data_type": "decimal",
            "precision": ((atttypmod - 4) >> 16) & 65535,
            "scale": (atttypmod - 4) & 65535,
        }
    if type_id == 1082:
        return {"data_type": "date"}
    if type_id in (1083, 1266):
        return {"data_type": "time"}
    if type_id == 1114:
        return {"data_type": "timestamp", "timezone": False}
    if type_id == 1184:
        return {"data_type": "timestamp", "timezone": True}
    if type_id in (1042, 1043) and atttypmod != -1:
        return {"data_type": "text", "precision": atttypmod - 4}
    return {"data_type": "text"}


def to_dlt_column_schema(
    col: ColumnType, pg_type: PgType, primary_key: bool
) -> TColumnSchema:
    column: TColumnSchema = {"name": col.name, **to_dlt_column_type(pg_type)}
    column["primary_key"] = primary_key
    return column


def _to_bytes(value: str) -> bytes:
    # bytea is sent in hex format, `bytea_output` is pinned on the replication connection
    return bytes.fromhex(value[2:])


def _to_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        if value == "infinity":
            return date.max
        if value == "-infinity":
            return date.min
        return coerce_value("date", "text", value)  # type: ignore[no-any-return]


def _to_time(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError:
        return coerce_value("time", "text", value)  # type: ignore[no-any-return]


def _to_timetz(value: str) -> time:
    sign_at = max(value.rfind("+"), value.rfind("-"))
    hours, minutes, seconds = (value[sign_at + 1 :].split(":") + ["0", "0"])[:3]
    offset = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))
    tz = timezone(-offset if value[sign_at] == "-" else offset)
    return _to_time(value[:sign_at]).replace(tzinfo=tz)


def _to_timestamp(value: str, tz: Optional[timezone]) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        if value == "infinity":
            return datetime.max.replace(tzinfo=tz)
        if value == "-infinity":
            return datetime.min.replace(tzinfo=tz)
        # python < 3.11 parses only 3 or 6 fractional digits and no `+HH` offsets
        return coerce_value("timestamp", "text", value)  # type: ignore[no-any-return]


def parse_array(value: str, convert: Optional[TValueConverter] = None) -> List[Any]:
    """Parses a Postgres array literal ie. `{1,NULL,"a b"}` into a (nested) list."""
    if value[0] == "[":
        # literal with explicit bounds ie. `[0:1]={1,2}`
        value = value[value.index("=") + 1 :]
    stack: List[List[Any]] = []
    result: List[Any] = []
    i, n = 0, len(value)
    while i < n:
        c = value[i]
        if c == "{":
            level: List[Any] = []
            if stack:
                stack[-1].append(level)
            stack.append(level)
            i += 1
        elif c == "}":
            result = stack.pop()
            i += 1
        elif c == ",":
            i += 1
        elif c == '"':
            chars = []
            i += 1
            while value[i] != '"':
                if value[i] == "\\":
                    i += 1
                chars.append(value[i])
                i += 1
            element = "".join(chars)
            stack[-1].append(convert(element) if convert else element)
            i += 1
        else:
            j = i
            while value[j] not in ",}":
                j += 1
            element = value[i:j]
            if element == "NULL":
                stack[-1].append(None)
            else:
                stack[-1].append(convert(element) if convert else element)
            i = j
    return result


_CONVERTERS: Dict[int, TValueConverter] = {
    16: "t".__eq__,
    17: _to_bytes,
    20: int,
    21: int,
    23: int,
    26: int,
    700: float,
    701: float,
    1700: Decimal,
    114: json.loads,
    3802: json.loads,
    1082: _to_date,
    1083: _to_time,
    1266: _to_timetz,
    1114: partial(_to_timestamp, tz=None),
    1184: partial(_to_timestamp, tz=timezone.utc),
}


def get_value_converter(pg_type: PgType) -> Optional[TValueConverter]:
    """Returns function converting text value to python value, None if text is kept."""
    if pg_type.element_type_id is not None:
        return partial(parse_array, convert=_CONVERTERS.get(pg_type.element_type_id))
    return _CONVERTERS.get(pg_type.type_id)


def warn_unknown_type(type_id: int, type_name: str) -> None:
    logger.warning(
        f"Type `{type_name}` (OID {type_id}) has no dlt counterpart and will be loaded as `text`."
    )
