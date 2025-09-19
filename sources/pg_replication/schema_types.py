from functools import lru_cache
import json
from typing import Optional, Any, Dict

from dlt.common import Decimal, logger
from dlt.common.data_types.typing import TDataType
from dlt.common.data_types.type_helpers import coerce_value
from dlt.common.schema.typing import TColumnSchema, TColumnType

from dlt.destinations.impl.postgres.factory import PostgresTypeMapper

from .decoders import ColumnType


_DUMMY_VALS: Dict[TDataType, Any] = {
    "bigint": 0,
    "binary": b" ",
    "bool": True,
    "json": [0],
    "date": "2000-01-01",
    "decimal": Decimal(0),
    "double": 0.0,
    "text": "",
    "time": "00:00:00",
    "timestamp": "2000-01-01T00:00:00",
    "wei": 0,
}
"""Dummy values used to replace NULLs in NOT NULL columns in key-only delete records."""


_PG_TYPES: Dict[int, str] = {
    16: "boolean",
    17: "bytea",
    20: "bigint",
    21: "smallint",
    23: "integer",
    701: "double precision",
    1043: "character varying",
    1082: "date",
    1083: "time without time zone",
    1114: "timestamp without time zone",
    1184: "timestamp with time zone",
    1700: "numeric",
    3802: "jsonb",
    114: "json",
}
"""Maps postgres type OID to type string. Only includes types present in PostgresTypeMapper."""


def _get_precision(type_id: int, atttypmod: int) -> Optional[int]:
    """Get precision from postgres type attributes."""
    # https://stackoverflow.com/a/3351120
    if type_id == 21:  # smallint
        return 16
    elif type_id == 23:  # integer
        return 32
    elif type_id == 20:  # bigint
        return 64
    if atttypmod != -1:
        if type_id == 1700:  # numeric
            return ((atttypmod - 4) >> 16) & 65535
        elif type_id in (
            1083,
            1184,
        ):  # time without time zone, timestamp with time zone
            return atttypmod
        elif type_id == 1043:  # character varying
            return atttypmod - 4
    return None


def _get_scale(type_id: int, atttypmod: int) -> Optional[int]:
    """Get scale from postgres type attributes."""
    # https://stackoverflow.com/a/3351120
    if atttypmod != -1:
        if type_id in (21, 23, 20):  # smallint, integer, bigint
            return 0
        if type_id == 1700:  # numeric
            return (atttypmod - 4) & 65535
    return None


@lru_cache(maxsize=None)
def _type_mapper() -> PostgresTypeMapper:
    from dlt.destinations import postgres

    return PostgresTypeMapper(postgres().capabilities())


def _to_dlt_column_type(type_id: int, atttypmod: int) -> TColumnType:
    """Converts postgres type OID to dlt column type.

    Type OIDs not in _PG_TYPES mapping default to "text" type.
    """
    pg_type = _PG_TYPES.get(type_id)
    if pg_type is None:
        logger.warning(
            f"Type OID {type_id} is unknown and will be converted to `text` dlt type"
        )
        pg_type = "character varying"
    precision = _get_precision(type_id, atttypmod)
    scale = _get_scale(type_id, atttypmod)
    dlt_type = _type_mapper().from_destination_type(pg_type, precision, scale)

    # Set timezone flag for timestamp types
    if type_id == 1114:  # timestamp without time zone
        dlt_type["timezone"] = False
    elif type_id == 1184:  # timestamp with time zone
        dlt_type["timezone"] = True
    return dlt_type


def _to_dlt_column_schema(col: ColumnType) -> TColumnSchema:
    """Converts pypgoutput ColumnType to dlt column schema."""
    dlt_column_type = _to_dlt_column_type(col.type_id, col.atttypmod)
    partial_column_schema = {
        "name": col.name,
        "primary_key": bool(col.part_of_pkey),
    }
    return {**dlt_column_type, **partial_column_schema}  # type: ignore[typeddict-item]


def _to_dlt_val(val: str, data_type: TDataType, byte1: str, for_delete: bool) -> Any:
    """Converts pgoutput's text-formatted value into dlt-compatible data value."""
    if byte1 == "n":
        if for_delete:
            # replace None with dummy value to prevent NOT NULL violations in staging table
            return _DUMMY_VALS[data_type]
        return None
    elif byte1 == "t":
        if data_type == "binary":
            # https://www.postgresql.org/docs/current/datatype-binary.html#DATATYPE-BINARY-BYTEA-HEX-FORMAT
            return bytes.fromhex(val.replace("\\x", ""))
        elif data_type == "json":
            return json.loads(val)
        return coerce_value(data_type, "text", val)
    else:
        raise ValueError(
            f"Byte1 in replication message must be 'n' or 't', not '{byte1}'."
        )
