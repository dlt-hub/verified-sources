from typing import Any, Dict, List, Mapping, Optional, Sequence

from dlt.common.schema.typing import TColumnSchema, TDataType

from .columns import MAX_DECIMAL_PRECISION, TConversion, TFields, TPath, TypedField
from .kafka_connect import VARIABLE_SCALE_DECIMAL, logical_field

PRIMITIVE_TYPES: Dict[str, TDataType] = {
    "boolean": "bool",
    "int": "bigint",
    "long": "bigint",
    "float": "double",
    "double": "double",
    "string": "text",
    "bytes": "binary",
}
INTEGER_TYPES = {"int", "long"}
NUMBER_TYPES = {"int", "long", "float", "double"}
# logical types fastavro converts, timestamps by whether they are in UTC
FASTAVRO_TIMESTAMPS = {
    ("long", "timestamp-millis"): True,
    ("long", "timestamp-micros"): True,
    ("long", "local-timestamp-millis"): False,
    ("long", "local-timestamp-micros"): False,
}
FASTAVRO_TIMES = {("int", "time-millis"), ("long", "time-micros")}
# nanoseconds load as microseconds: dlt writes microseconds to parquet unless configured otherwise
NANO_TIMESTAMPS = {
    ("long", "timestamp-nanos"): True,
    ("long", "local-timestamp-nanos"): False,
}
CONNECT_DEFAULT_NAME = "io.confluent.connect.avro.ConnectDefault"
VALUE_COLUMN = "value"


def avro_fields(
    schema: Any,
    named: Mapping[str, Any],
    prefix: TPath = (),
    scalar_name: Optional[str] = VALUE_COLUMN,
) -> TFields:
    """Columns of the values of an Avro schema, with the conversions of decoded values.

    Records are flattened into nested columns. Arrays, maps, unions of several types and
    recursive references are `json`. A schema that is not a record loads into one column,
    `scalar_name` under `prefix`, or `prefix` itself when `scalar_name` is None.

    Args:
        schema: Avro schema parsed by fastavro, which names types by their full names.
        named: The named types of the schema by full name, the `named_schemas` of fastavro.
        prefix: Path of the value in the rows.
        scalar_name: Column of a schema that is not a record.
    """
    walker = _Walker(named)
    root = _resolve(schema, named)
    if _record_name(root):
        walker.add_record(prefix, root)
    else:
        walker.add(prefix + (scalar_name,) if scalar_name else prefix, root)
    return walker.fields


def union_fields(branches: Sequence[Any], named: Mapping[str, Any]) -> TFields:
    """Fields of all branches of a top-level union, which load into one table. A field with
    other types in other branches loads as json."""
    fields: TFields = {}
    for branch in branches:
        for path, field in avro_fields(branch, named).items():
            seen = fields.setdefault(path, field)
            if seen != field:
                fields[path] = TypedField({"data_type": "json"}, None)
    return fields


def record_name(schema: Any, named: Mapping[str, Any]) -> Optional[str]:
    """Full name of a named schema, `None` for Kafka Connect default names"""
    root = _resolve(schema, named)
    if not isinstance(root, dict) or "name" not in root:
        return _type_name(root)
    name: str = root["name"]
    return None if name.startswith(CONNECT_DEFAULT_NAME) else name


def is_record(schema: Any, named: Mapping[str, Any]) -> bool:
    """Values of `schema` are records whose fields are columns"""
    return _record_name(_resolve(schema, named)) is not None


def decode_schema(schema: Any) -> Any:
    """Copy of `schema` that fastavro parses: invalid decimal logical types, which the Avro
    specification says fall back to their type, and field defaults are removed. Values are
    decoded with the writer schema, so defaults are never used."""
    if isinstance(schema, list):
        return [decode_schema(branch) for branch in schema]
    if not isinstance(schema, dict):
        return schema
    copy: Dict[str, Any] = {}
    for key, value in schema.items():
        if key in ("type", "items", "values"):
            copy[key] = decode_schema(value)
        elif key == "fields":
            copy[key] = [
                {
                    k: decode_schema(v) if k == "type" else v
                    for k, v in field.items()
                    if k != "default"
                }
                for field in value
            ]
        else:
            copy[key] = value
    if copy.get("logicalType") == "decimal" and _decimal_precision(copy) is None:
        del copy["logicalType"]
    return copy


class _Walker:
    def __init__(self, named: Mapping[str, Any]) -> None:
        self.named = named
        self.fields: TFields = {}
        self.stack: List[str] = []

    def add(self, path: TPath, schema: Any) -> None:
        schema = _resolve(schema, self.named)
        if isinstance(schema, list):
            self.add_union(path, schema)
            return
        type_name = _type_name(schema)
        if type_name in ("record", "error"):
            full_name = _record_name(schema)
            if full_name is None:
                self.add_column(path, "text", ("variable_decimal", None))
            elif full_name in self.stack:
                # recursive types cannot be flattened
                self.add_column(path, "json", None)
            else:
                self.add_record(path, schema)
        elif type_name in ("array", "map"):
            self.add_column(path, "json", None)
        elif type_name != "null":
            self.add_scalar(path, schema)

    def add_record(self, path: TPath, schema: Mapping[str, Any]) -> None:
        self.stack.append(schema["name"])
        for field in schema.get("fields", []):
            self.add(path + (field["name"],), field["type"])
        self.stack.pop()

    def add_union(self, path: TPath, branches: List[Any]) -> None:
        not_null = [
            branch
            for branch in (_resolve(b, self.named) for b in branches)
            if _type_name(branch) != "null"
        ]
        if len(not_null) == 1:
            self.add(path, not_null[0])
            return
        if not not_null:
            return
        # dlt has no union type: a union of several types other than numbers loads as json
        types = [_plain_type(branch) for branch in not_null]
        if all(t in INTEGER_TYPES for t in types):
            self.add_column(path, "bigint", None)
        elif all(t in NUMBER_TYPES for t in types):
            self.add_column(path, "double", None)
        else:
            self.add_column(path, "json", None)

    def add_scalar(self, path: TPath, schema: Any) -> None:
        type_name = _type_name(schema)
        properties: Mapping[str, Any] = schema if isinstance(schema, dict) else {}
        logical_type = properties.get("logicalType")
        key = (type_name, logical_type)
        precision = (
            _decimal_precision(properties) if logical_type == "decimal" else None
        )
        logical = logical_field(properties.get("connect.name"), type_name)
        if precision is not None and precision > MAX_DECIMAL_PRECISION:
            self.add_column(path, "text", ("str", None))
        elif precision is not None:
            scale = int(properties.get("scale", 0))
            self.add_column(path, "decimal", None, precision=precision, scale=scale)
        elif logical_type == "uuid" and (
            type_name == "string"
            or (type_name == "fixed" and properties.get("size") == 16)
        ):
            self.add_column(path, "text", ("uuid", None))
        elif key == ("int", "date"):
            self.add_column(path, "date", None)
        elif key in FASTAVRO_TIMES:
            self.add_column(path, "time", None)
        elif key in FASTAVRO_TIMESTAMPS:
            self.add_column(path, "timestamp", None, timezone=FASTAVRO_TIMESTAMPS[key])
        elif key in NANO_TIMESTAMPS:
            utc = NANO_TIMESTAMPS[key]
            kind = "epoch" if utc else "local_epoch"
            self.add_column(path, "timestamp", (kind, "ns"), timezone=utc)
        elif logical is not None:
            data_type, conversion, hints = logical
            self.add_column(path, data_type, conversion, **hints)
        elif type_name == "enum":
            self.add_column(path, "text", None)
        elif type_name == "fixed":
            self.add_column(path, "binary", None)
        else:
            # other logical types load as their underlying type, as the Avro specification says
            self.add_column(path, PRIMITIVE_TYPES[type_name], None)

    def add_column(
        self,
        path: TPath,
        data_type: TDataType,
        conversion: Optional[TConversion],
        **hints: Any,
    ) -> None:
        column: TColumnSchema = {"data_type": data_type}
        column.update(hints)  # type: ignore[typeddict-item]
        self.fields[path] = TypedField(column, conversion)


def _resolve(schema: Any, named: Mapping[str, Any]) -> Any:
    """Definition of a named type reference, a type wrapped in `{"type": ...}` unwrapped"""
    if isinstance(schema, str) and schema in named:
        return _resolve(named[schema], named)
    if isinstance(schema, dict) and isinstance(schema.get("type"), (dict, list)):
        return _resolve(schema["type"], named)
    return schema


def _type_name(schema: Any) -> str:
    if isinstance(schema, list):
        return "union"
    return schema if isinstance(schema, str) else str(schema.get("type"))


def _plain_type(schema: Any) -> Optional[str]:
    """Primitive type of a schema without a logical type, `None` for other schemas"""
    if isinstance(schema, str):
        return schema
    if not {"logicalType", "connect.name"} & schema.keys():
        return _type_name(schema)
    return None


def _record_name(schema: Any) -> Optional[str]:
    """Full name of a record whose fields are columns: not a Debezium VariableScaleDecimal"""
    if _type_name(schema) not in ("record", "error"):
        return None
    if VARIABLE_SCALE_DECIMAL in (schema["name"], schema.get("connect.name")):
        return None
    return str(schema["name"])


def _decimal_precision(schema: Mapping[str, Any]) -> Optional[int]:
    """Precision of a valid decimal, `None` for invalid ones"""
    precision = schema.get("precision")
    scale = schema.get("scale", 0)
    if not isinstance(precision, int) or not isinstance(scale, int):
        return None
    if precision <= 0 or scale < 0 or scale > precision:
        return None
    if schema.get("type") == "fixed":
        size = schema.get("size", 0)
        if precision > len(str(2 ** (8 * size - 1) - 1)) - 1:
            return None
    return precision
