import base64
from datetime import date, datetime, time, timedelta, timezone
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from uuid import UUID

from dlt.common import Decimal, json
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.time import ensure_datetime_in_tz

TPath = Tuple[str, ...]
"""Keys of a field in a decoded value, from the top"""
TConversion = Tuple[str, Any]
"""How a decoded value is converted, a kind and its parameter, see `value_converter`"""

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
# local timestamps are wall-clock times without a time zone
LOCAL_EPOCH = datetime(1970, 1, 1)


class TypedField(NamedTuple):
    """A column of a schema field and how its decoded values are converted"""

    column: TColumnSchema
    conversion: Optional[TConversion]


TFields = Dict[TPath, TypedField]


def column_name(path: TPath, naming: NamingConvention) -> str:
    """Normalized name of the column of `path`, as dlt names the leaves of nested dicts"""
    return naming.normalize_path(naming.make_path(*path))


def fields_columns(fields: TFields, naming: NamingConvention) -> TTableSchemaColumns:
    """Columns of `fields`, named with the path separator of `naming`"""
    columns: TTableSchemaColumns = {}
    for path, field in fields.items():
        name = naming.make_path(*path)
        columns[name] = {**field.column, "name": name}
    return columns


def check_names(fields: TFields, naming: NamingConvention) -> None:
    """Raises when two fields load into the same column"""
    seen: Dict[str, TPath] = {}
    for path in fields:
        name = column_name(path, naming)
        if name in seen:
            raise ValueError(
                f"Fields `{'.'.join(seen[name])}` and `{'.'.join(path)}` load into the same"
                f" column `{name}`"
            )
        seen[name] = path


def value_converter(conversion: TConversion) -> Callable[[Any], Any]:
    """Converts a decoded value to the Python value of its column"""
    kind, parameter = conversion
    if kind == "epoch":
        return partial(_epoch, parameter, True)
    if kind == "local_epoch":
        return partial(_epoch, parameter, False)
    if kind == "time":
        return partial(_time, parameter)
    if kind == "days":
        return _days
    if kind == "base64":
        return base64.b64decode
    if kind == "decimal":
        return partial(_decimal, scale=parameter)
    if kind == "variable_decimal":
        return _variable_decimal
    if kind == "uuid":
        return _uuid
    if kind == "str":
        return str
    if kind == "iso_timestamp":
        return partial(ensure_datetime_in_tz, tz=timezone.utc)
    if kind == "json":
        return _json_text
    raise ValueError(f"Unknown conversion `{kind}`")


def row_conversions(
    fields: TFields, python_rows: bool
) -> Dict[TPath, Callable[[Any], Any]]:
    """Conversions of decoded values to the values of their columns, by path"""
    conversions: Dict[TPath, Callable[[Any], Any]] = {}
    for path, field in fields.items():
        convert = value_converter(field.conversion) if field.conversion else None
        if field.column.get("data_type") == "json":
            is_text = field.conversion is not None and field.conversion[0] == "json"
            conversions[path] = partial(_json_value, convert, is_text, python_rows)
        elif convert is not None:
            conversions[path] = convert
    return conversions


def convert_row(
    row: Dict[str, Any], conversions: Mapping[TPath, Callable[[Any], Any]]
) -> None:
    """Converts the values of `row` in place"""
    for path, convert in conversions.items():
        parent = _parent(row, path)
        if parent is not None and parent.get(path[-1]) is not None:
            parent[path[-1]] = convert(parent[path[-1]])


def record_paths(fields: TFields) -> List[TPath]:
    """Paths of the records that hold `fields`, longest first"""
    paths = {path[:end] for path in fields for end in range(1, len(path))}
    return sorted(paths, key=len, reverse=True)


def drop_null_records(row: Dict[str, Any], paths: Sequence[TPath]) -> None:
    """Removes records that are `None` from `row`: they have no column of their own"""
    for path in paths:
        parent = _parent(row, path)
        if parent is not None and path[-1] in parent and parent[path[-1]] is None:
            del parent[path[-1]]


def _parent(row: Dict[str, Any], path: TPath) -> Optional[Dict[str, Any]]:
    node: Any = row
    for key in path[:-1]:
        node = node.get(key) if isinstance(node, dict) else None
    return node if isinstance(node, dict) else None


def _micros(unit: str, value: int) -> int:
    if unit == "ms":
        return value * 1000
    if unit == "us":
        return value
    return value // 1000


def _epoch(unit: str, utc: bool, value: int) -> datetime:
    # values past the year 9999, for example Debezium `infinity`, raise OverflowError
    return (EPOCH if utc else LOCAL_EPOCH) + timedelta(
        microseconds=_micros(unit, value)
    )


def _time(unit: str, value: int) -> time:
    return (datetime.min + timedelta(microseconds=_micros(unit, value))).time()


def _days(value: int) -> date:
    return date(1970, 1, 1) + timedelta(days=value)


def _decimal(value: Union[bytes, str], scale: int) -> Decimal:
    """Decimal from its unscaled big-endian bytes, base64 encoded in JSON"""
    data = base64.b64decode(value) if isinstance(value, str) else value
    return Decimal(int.from_bytes(data, "big", signed=True)).scaleb(-scale)


def _variable_decimal(value: Union[Mapping[str, Any], str]) -> str:
    """Debezium VariableScaleDecimal, a record of `scale` and `value` or its JSON"""
    record: Mapping[str, Any] = json.loads(value) if isinstance(value, str) else value
    return str(_decimal(record["value"], int(record["scale"])))


def _uuid(value: Union[UUID, bytes, str]) -> str:
    if isinstance(value, bytes):
        return str(UUID(bytes=value))
    return str(value)


def _json_text(value: str) -> str:
    """`value` when it is JSON, else a JSON string of it, for example the placeholder Debezium
    writes for a value that did not change"""
    try:
        json.loads(value)
    except ValueError:
        return json.dumps(value)
    return value


def _json_value(
    convert: Optional[Callable[[Any], Any]],
    is_text: bool,
    python_rows: bool,
    value: Any,
) -> Any:
    """Value of a json column: JSON text in Arrow tables. In rows, dlt parses text into json
    columns and writes back text as it is, so scalars are encoded twice."""
    if convert is not None:
        value = convert(value)
    if not python_rows:
        return value if is_text else json.dumps(value)
    if is_text:
        value = json.loads(value)
    if isinstance(value, (dict, list)):
        return value
    return json.dumps(json.dumps(value))
