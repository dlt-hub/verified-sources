from typing import Any, Dict, Mapping, Optional, Tuple

from dlt.common.schema.typing import TColumnSchema, TDataType

from .columns import MAX_DECIMAL_PRECISION, TConversion, TFields, TPath, TypedField

ENVELOPE_PREFIX = b'{"schema":'
ENVELOPE_PAYLOAD = b',"payload":'

DECIMAL = "org.apache.kafka.connect.data.Decimal"
VARIABLE_SCALE_DECIMAL = "io.debezium.data.VariableScaleDecimal"
ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp"
JSON_STRING = "io.debezium.data.Json"
TIMESTAMPS = {
    "org.apache.kafka.connect.data.Timestamp": "ms",
    "io.debezium.time.Timestamp": "ms",
    "io.debezium.time.MicroTimestamp": "us",
    "io.debezium.time.NanoTimestamp": "ns",
}
TIMES = {
    "org.apache.kafka.connect.data.Time": "ms",
    "io.debezium.time.Time": "ms",
    "io.debezium.time.MicroTime": "us",
    "io.debezium.time.NanoTime": "ns",
}
DATES = {"org.apache.kafka.connect.data.Date", "io.debezium.time.Date"}
# primitive types of Kafka Connect and Avro, logical names apply only to their base types
LONG_TYPES = {"int64", "long"}
INT_TYPES = {"int32", "int"} | LONG_TYPES
STRING_TYPES = {"string"}
CONNECT_TYPES: Dict[str, TDataType] = {
    "int8": "bigint",
    "int16": "bigint",
    "int32": "bigint",
    "int64": "bigint",
    "float32": "double",
    "float64": "double",
    "boolean": "bool",
    "string": "text",
}


def logical_field(
    name: Optional[str], primitive: Any
) -> Optional[Tuple[TDataType, TConversion, Dict[str, Any]]]:
    """Data type, conversion and hints of a Kafka Connect or Debezium logical type name on a
    Kafka Connect or Avro primitive type, `None` for other types"""
    if name in TIMESTAMPS and primitive in LONG_TYPES:
        return "timestamp", ("epoch", TIMESTAMPS[name]), {"timezone": True}
    if name in TIMES and primitive in INT_TYPES:
        return "time", ("time", TIMES[name]), {}
    if name in DATES and primitive in INT_TYPES - LONG_TYPES:
        return "date", ("days", None), {}
    if name == ZONED_TIMESTAMP and primitive in STRING_TYPES:
        return "timestamp", ("iso_timestamp", None), {"timezone": True}
    if name == JSON_STRING and primitive in STRING_TYPES:
        return "json", ("json", None), {}
    return None


def connect_payload(value: bytes) -> Optional[bytes]:
    """The payload of a Kafka Connect JSON envelope, `None` when `value` is not one.

    `JsonConverter` writes the schema first, so the payload starts at the first `,"payload":`.
    """
    if not value.startswith(ENVELOPE_PREFIX):
        return None
    start = value.find(ENVELOPE_PAYLOAD)
    if start < 0:
        return None
    return value[start + len(ENVELOPE_PAYLOAD) : value.rindex(b"}")]


def connect_schema(value: bytes) -> Optional[bytes]:
    """The schema of a Kafka Connect JSON envelope, `None` when `value` is not one or has no
    schema, as `JsonConverter` writes schemaless records"""
    if not value.startswith(ENVELOPE_PREFIX):
        return None
    end = value.find(ENVELOPE_PAYLOAD)
    if end < 0:
        return None
    schema = value[len(ENVELOPE_PREFIX) : end]
    return None if schema.strip() == b"null" else schema


def connect_fields(schema: Mapping[str, Any]) -> TFields:
    """Columns and value conversions of the flattened fields of a Kafka Connect struct schema"""
    fields: TFields = {}
    _add_fields(fields, (), schema)
    return fields


def _add_fields(fields: TFields, path: TPath, schema: Mapping[str, Any]) -> None:
    connect_type = schema.get("type")
    name = schema.get("name")
    if connect_type == "struct" and name != VARIABLE_SCALE_DECIMAL:
        for child in schema.get("fields", []):
            _add_fields(fields, path + (child["field"],), child)
        return
    if not path:
        return
    column: TColumnSchema = {"data_type": "json"}
    conversion: Optional[TConversion] = None
    logical = logical_field(name, connect_type)
    if logical is not None:
        data_type, conversion, hints = logical
        column = {"data_type": data_type, **hints}  # type: ignore[typeddict-item]
    elif name == DECIMAL:
        parameters = schema.get("parameters") or {}
        scale = int(parameters.get("scale", 0))
        precision = max(
            int(parameters.get("connect.decimal.precision", MAX_DECIMAL_PRECISION)),
            scale,
        )
        if precision > MAX_DECIMAL_PRECISION:
            column = {"data_type": "text"}
            conversion = ("decimal_text", scale)
        else:
            column = {"data_type": "decimal", "precision": precision, "scale": scale}
            conversion = ("decimal", scale)
    elif name == VARIABLE_SCALE_DECIMAL:
        column["data_type"] = "text"
        conversion = ("variable_decimal", None)
    elif connect_type == "bytes":
        column["data_type"] = "binary"
        conversion = ("base64", None)
    elif connect_type in CONNECT_TYPES:
        column["data_type"] = CONNECT_TYPES[connect_type]
    fields[path] = TypedField(column, conversion)
