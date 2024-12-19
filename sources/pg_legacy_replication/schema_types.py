import re
from functools import lru_cache
from typing import Any, Callable, Dict, Optional, Tuple

import pendulum
from dlt.common import Decimal, logger
from dlt.common.data_types.type_helpers import coerce_value
from dlt.common.data_types.typing import TDataType
from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.destinations.impl.postgres.factory import PostgresTypeMapper

from .pg_logicaldec_pb2 import DatumMessage, TypeInfo

_DUMMY_VALS: Dict[TDataType, Any] = {
    "bigint": 0,
    "binary": b" ",
    "bool": True,
    "json": [0],
    "date": pendulum.Date(1970, 1, 1),
    "decimal": Decimal(0),
    "double": 0.0,
    "text": "",
    "time": pendulum.Time(0, 0, 0),
    "timestamp": pendulum.from_timestamp(0),
    "wei": 0,
}
"""Dummy values used to replace NULLs in NOT NULL columns in key-only delete records."""

_PG_TYPES: Dict[int, str] = {
    16: "boolean",
    17: "bytea",
    20: "bigint",
    21: "smallint",
    23: "integer",
    25: "text",
    700: "real",
    701: "double precision",
    1043: "character varying",
    1082: "date",
    1083: "time without time zone",
    1114: "timestamp without time zone",
    1184: "timestamp with time zone",
    1700: "numeric",
    3802: "jsonb",
}
"""Maps postgres type OID to type string."""

_MISSING_TYPES: Dict[str, TDataType] = {
    "real": "double",
    "text": "text",
    "timestamp without time zone": "timestamp",
}
# FIXME Missing types for old postgres versions

_DATUM_RAW_TYPES: Dict[str, TDataType] = {
    "datum_int32": "bigint",
    "datum_int64": "bigint",
    "datum_float": "double",
    "datum_double": "double",
    "datum_bool": "bool",
    "datum_string": "text",
    "datum_bytes": "binary",
}
"""Maps decoderbuf's datum msg type to dlt type."""

_FIXED_PRECISION_TYPES: Dict[int, Tuple[int, Optional[int]]] = {
    21: (16, None),  # smallint
    23: (32, None),  # integer
    20: (64, None),  # bigint
    700: (32, None),  # real
}
"""Dict for fixed precision types"""

_VARYING_PRECISION_PATTERNS: Dict[int, str] = {
    1043: r"character varying\((\d+)\)",
    1700: r"numeric\((\d+),(\d+)\)",
    1184: r"timestamp\((\d+)\) with time zone",
    1083: r"time\((\d+)\) without time zone",
}
"""Regex patterns for precision/scale types"""


def _get_precision_and_scale(
    type_id: int, modifier: Optional[str]
) -> Optional[Tuple[Optional[int], Optional[int]]]:
    """Get precision from postgres type attributes and modifiers."""
    if type_id in _FIXED_PRECISION_TYPES:
        return _FIXED_PRECISION_TYPES[type_id]

    # If modifier or pattern is missing, return defaults
    if not modifier or (pattern := _VARYING_PRECISION_PATTERNS.get(type_id)) is None:
        return None, None

    if match := re.search(pattern, modifier):
        groups = match.groups()
        precision = int(groups[0])
        scale = int(groups[1]) if len(groups) > 1 else None
        return precision, scale

    return None, None


@lru_cache(maxsize=None)
def _type_mapper() -> PostgresTypeMapper:
    from dlt.destinations import postgres

    return PostgresTypeMapper(postgres().capabilities())


def _to_dlt_column_type(type_id: int, modifier: Optional[str]) -> TColumnType:
    """Converts postgres type OID to dlt column type.

    Type OIDs not in _PG_TYPES mapping default to "text" type.
    """
    pg_type = _PG_TYPES.get(type_id)
    if pg_type in _MISSING_TYPES:
        return {"data_type": _MISSING_TYPES[pg_type]}
    if pg_type is None:
        logger.warning(
            "No type found for type_id '%s' and modifier '%s'", type_id, modifier
        )

    precision, scale = _get_precision_and_scale(type_id, modifier)
    return _type_mapper().from_destination_type(pg_type, precision, scale)


def _to_dlt_column_schema(
    col_name: str, datum: DatumMessage, type_info: Optional[TypeInfo]
) -> TColumnSchema:
    """Converts decoderbuf's datum value/typeinfo to dlt column schema."""
    column_schema: TColumnSchema = {
        "name": col_name,
        **_to_dlt_column_type(
            datum.column_type, type_info.modifier if type_info else None
        ),
    }

    # Set nullable attribute if type_info is available
    if type_info:
        column_schema["nullable"] = type_info.value_optional
    elif datum.WhichOneof("datum"):  # Or simply guess as this is a very rare case
        column_schema["nullable"] = False

    return column_schema


def _epoch_micros_to_datetime(microseconds_since_1970: int) -> pendulum.DateTime:
    return pendulum.from_timestamp(microseconds_since_1970 / 1_000_000)


def _microseconds_to_time(microseconds: int) -> pendulum.Time:
    return pendulum.Time(0).add(microseconds=microseconds)


def _epoch_days_to_date(epoch_days: int) -> pendulum.Date:
    return pendulum.Date(1970, 1, 1).add(days=epoch_days)


data_type_handlers: Dict[TDataType, Callable[[Any], Any]] = {
    "date": _epoch_days_to_date,
    "time": _microseconds_to_time,
    "timestamp": _epoch_micros_to_datetime,
}
"""Dispatch table for type conversions"""


def _to_dlt_val(
    val: DatumMessage, data_type: TDataType, *, for_delete: bool = False
) -> Any:
    """Converts decoderbuf's datum value into dlt-compatible data value."""
    datum = val.WhichOneof("datum")
    if datum is None:
        return _DUMMY_VALS[data_type] if for_delete else None
    if datum == "datum_missing":
        return None

    raw_value = getattr(val, datum)
    if data_type in data_type_handlers:
        return data_type_handlers[data_type](raw_value)

    raw_type = _DATUM_RAW_TYPES[datum]
    return coerce_value(data_type, raw_type, raw_value)
