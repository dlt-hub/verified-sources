import io
from datetime import datetime, timedelta, timezone
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import pyarrow as pa
import pyarrow.compute as pc
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Message
from dlt.common import json
from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.libs.pyarrow import cast_arrow_as_columns_schema, get_py_arrow_datatype
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema.typing import TTableSchemaColumns
from pyarrow import json as pa_json

from .columns import EPOCH, TFields, TPath, column_name, value_converter
from .processors import MessageDecodeError, TPayload, message_id, text_key

UTC_MICROS = pa.timestamp("us", tz="UTC")
# JSON Schema types as pyarrow.json parses them, dates, times and timestamps are converted from
# strings: pyarrow rejects timestamps with more than 6 fractional digits
PARSE_TYPES: Dict[str, pa.DataType] = {
    "bigint": pa.int64(),
    "double": pa.float64(),
    "bool": pa.bool_(),
    "text": pa.string(),
    "timestamp": pa.string(),
    "date": pa.string(),
    "time": pa.string(),
}
MIN_BLOCK_SIZE = 1 << 20
# the range of Python datetimes in microseconds and days since the epoch
MIN_MICROS = (datetime(1, 1, 1, tzinfo=timezone.utc) - EPOCH) // timedelta(
    microseconds=1
)
MAX_MICROS = (datetime.max.replace(tzinfo=timezone.utc) - EPOCH) // timedelta(
    microseconds=1
)
MICROS_PER_UNIT = {"ms": 1000, "us": 1}

TArrowTypes = Dict[TPath, pa.DataType]
"""Arrow types of the fields of a table by their paths"""


def schema_types(fields: TFields) -> TArrowTypes:
    """Arrow types to parse the fields of a JSON Schema, `json` fields are left out"""
    types: TArrowTypes = {}
    for path, field in fields.items():
        arrow_type = PARSE_TYPES.get(field.column.get("data_type"))
        if arrow_type is not None:
            types[path] = arrow_type
    return types


def connect_types(fields: TFields) -> TArrowTypes:
    """Arrow types to parse Kafka Connect payloads, converted values are parsed as JSON has them"""
    types: TArrowTypes = {}
    for path, field in fields.items():
        kind = field.conversion[0] if field.conversion else None
        if kind in ("epoch", "time", "days"):
            types[path] = pa.int64()
        elif kind in ("base64", "decimal", "decimal_text", "json", "iso_timestamp"):
            types[path] = pa.string()
        elif kind is None:
            arrow_type = PARSE_TYPES.get(field.column.get("data_type"))
            if arrow_type is not None:
                types[path] = arrow_type
    return types


def nested_schema(types: Mapping[TPath, pa.DataType]) -> pa.Schema:
    """Schema with nested structs of the `types` paths"""
    tree: Dict[str, Any] = {}
    for path, arrow_type in types.items():
        *parents, leaf = path
        node = tree
        for parent in parents:
            child = node.setdefault(parent, {})
            if not isinstance(child, dict):
                break
            node = child
        else:
            node.setdefault(leaf, arrow_type)

    def fields(node: Dict[str, Any]) -> List[pa.Field]:
        return [
            pa.field(
                name, pa.struct(fields(child)) if isinstance(child, dict) else child
            )
            for name, child in node.items()
        ]

    return pa.schema(fields(tree))


def iso_timestamps(array: Any) -> Any:
    """ISO 8601 strings as UTC timestamps, digits past microseconds are cut like in rows"""
    try:
        return pc.cast(array, UTC_MICROS)
    except pa.ArrowInvalid:
        convert = value_converter(("iso_timestamp", None))
        return pa.array(
            [None if value is None else convert(value) for value in array.to_pylist()],
            UTC_MICROS,
        )


def check_range(array: Any, low: int, high: int) -> None:
    """Raises when a value of `array` is outside of `low` and `high`, like Python datetimes do"""
    values = array.cast(pa.int64())
    if pc.any(pc.or_(pc.less(values, low), pc.greater(values, high))).as_py():
        raise ValueError("a date or timestamp is outside of the years 1 to 9999")


def convert_arrays(
    table: pa.Table, fields: TFields, naming: NamingConvention
) -> pa.Table:
    """Converts the values of `fields` with a conversion to the types of their columns"""
    for path, field in fields.items():
        name = column_name(path, naming)
        if field.conversion is None or name not in table.column_names:
            continue
        kind, parameter = field.conversion
        index = table.schema.get_field_index(name)
        array = table.column(index)
        if kind == "epoch":
            if parameter == "ns":
                check_range(array, MIN_MICROS * 1000, MAX_MICROS * 1000)
            else:
                unit = MICROS_PER_UNIT[parameter]
                check_range(array, MIN_MICROS // unit, MAX_MICROS // unit)
            array = array.cast(pa.int64()).cast(pa.timestamp(parameter, tz="UTC"))
            # nanoseconds load as microseconds, like in rows
            array = array.cast(UTC_MICROS, safe=False)
        elif kind == "time":
            if parameter == "ms":
                array = array.cast(pa.int32()).cast(pa.time32("ms"))
            else:
                array = array.cast(pa.int64()).cast(pa.time64(parameter))
                array = array.cast(pa.time64("us"), safe=False)
        elif kind == "days":
            check_range(
                array, MIN_MICROS // 86_400_000_000, MAX_MICROS // 86_400_000_000
            )
            array = array.cast(pa.int32()).cast(pa.date32())
        elif kind == "iso_timestamp":
            array = iso_timestamps(array)
        else:
            convert = value_converter(field.conversion)
            if kind == "decimal":
                arrow_type = pa.decimal128(
                    field.column["precision"], field.column["scale"]
                )
            else:
                arrow_type = pa.binary() if kind == "base64" else pa.string()
            array = pa.array(
                [
                    None if value is None else convert(value)
                    for value in array.to_pylist()
                ],
                arrow_type,
            )
        table = table.set_column(index, name, array)
    return table


def scalar_types(schema: pa.Schema) -> TArrowTypes:
    """Types of the scalar fields of a parsed table by their paths, structs are walked"""
    types: TArrowTypes = {}

    def add(field: pa.Field, path: TPath) -> None:
        if pa.types.is_struct(field.type):
            for child in field.type:
                add(child, path + (child.name,))
        elif not pa.types.is_null(field.type) and not pa.types.is_nested(field.type):
            types[path] = field.type

    for field in schema:
        add(field, (field.name,))
    return types


def json_strings(array: Any) -> Any:
    """Nested values as JSON strings, the Arrow type of dlt `json` columns"""
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    return pa.array(
        [None if value is None else json.dumps(value) for value in array.to_pylist()],
        pa.string(),
    )


def flatten_structs(
    table: pa.Table, naming: NamingConvention, json_columns: Set[str]
) -> pa.Table:
    """Flattens struct columns into nested columns like the relational normalizer flattens dicts.
    Lists and struct columns typed `json` become JSON strings."""
    names: List[str] = []
    arrays: List[Any] = []

    def add(array: Any, path: TPath) -> None:
        name = naming.shorten_fragments(*path)
        if pa.types.is_struct(array.type) and name not in json_columns:
            for field, child in zip(array.type, array.flatten()):
                add(child, path + (naming.normalize_path(field.name),))
        else:
            names.append(name)
            arrays.append(
                json_strings(array) if pa.types.is_nested(array.type) else array
            )

    for field, array in zip(table.schema, table.columns):
        add(array, (naming.normalize_path(field.name),))
    return pa.Table.from_arrays(arrays, names=names)


def metadata_columns(
    messages: List[Message],
    keys: Optional[Sequence[Union[str, bytes, None]]],
    naming: NamingConvention,
) -> Dict[str, Any]:
    """The `_kafka` columns and `_kafka_msg_id`, named as rows get them. Without `keys` the keys
    are decoded elsewhere and `_kafka_msg_id` hashes the raw keys."""
    timestamps = [msg.timestamp() for msg in messages]
    columns: Dict[TPath, Any] = {
        ("_kafka", "partition"): pa.array(
            [msg.partition() for msg in messages], pa.int64()
        ),
        ("_kafka", "topic"): pa.array([msg.topic() for msg in messages], pa.string()),
        ("_kafka", "offset"): pa.array([msg.offset() for msg in messages], pa.int64()),
        ("_kafka", "ts", "type"): pa.array(
            [ts_type for ts_type, _ in timestamps], pa.int64()
        ),
        ("_kafka", "ts", "value"): pa.array(
            [
                None if ts_type == TIMESTAMP_NOT_AVAILABLE else ts_ms * 1000
                for ts_type, ts_ms in timestamps
            ],
            pa.timestamp("us", tz="UTC"),
        ),
        ("_kafka_msg_id",): pa.array(
            [
                message_id(msg, key)
                for msg, key in zip(
                    messages, keys if keys is not None else [m.key() for m in messages]
                )
            ],
            pa.string(),
        ),
    }
    if keys is not None:
        columns[("_kafka", "key")] = pa.array(keys, pa.string())
    return {column_name(path, naming): array for path, array in columns.items()}


def destination_caps() -> DestinationCapabilitiesContext:
    return (
        Container().get(DestinationCapabilitiesContext)
        or DestinationCapabilitiesContext.generic_capabilities()
    )


def cast_to_columns(
    table: pa.Table, columns: Optional[TTableSchemaColumns], naming: NamingConvention
) -> pa.Table:
    """Casts the columns of `table` that have a data type in `columns`, except `json`"""
    typed = {}
    for name, column in (columns or {}).items():
        normalized = naming.normalize_path(name)
        if normalized in table.column_names and column.get("data_type") not in (
            None,
            "json",
        ):
            typed[normalized] = column
    if not typed:
        return table
    cast = cast_arrow_as_columns_schema(
        table.select(list(typed)), typed, destination_caps(), "UTC"
    )
    for name in typed:
        table = table.set_column(
            table.schema.get_field_index(name),
            cast.schema.field(name),
            cast.column(name),
        )
    return table


def json_arrow_table(
    messages: List[Message],
    payload: TPayload,
    types: TArrowTypes,
    columns: Optional[TTableSchemaColumns],
    allowed: Optional[Set[str]],
    naming: NamingConvention,
    on_error: Callable[[MessageDecodeError], None],
    fields: TFields,
) -> Optional[pa.Table]:
    """Arrow table of JSON object values, `None` when no message decodes.

    Args:
        messages: Messages of one topic and one schema.
        payload: Returns the JSON of a value.
        types: Arrow types of the fields of the table, parsed as explicit types. Types of new
            fields are added, so the table keeps its Arrow types: dlt cannot load a column whose
            Arrow type changes during a load.
        columns: Columns of the table, cast after parsing.
        allowed: Top-level fields allowed in strict mode.
        naming: Naming convention of the destination schema.
        on_error: Called with the messages that do not decode.
        fields: Fields of the payloads, the values of fields with a conversion are converted.
    """
    json_columns = {
        naming.normalize_path(name)
        for name, column in (columns or {}).items()
        if column.get("data_type") == "json"
    }
    json_columns.update(
        column_name(path, naming)
        for path, field in fields.items()
        if field.conversion and field.conversion[0] == "variable_decimal"
    )
    caps = destination_caps()
    converted = {
        column_name(path, naming) for path, field in fields.items() if field.conversion
    }
    # decimals parse from the JSON text, not through floats
    decimals = {
        naming.normalize_path(name): get_py_arrow_datatype(column, caps, "UTC")
        for name, column in (columns or {}).items()
        if column.get("data_type") == "decimal"
        and naming.normalize_path(name) not in converted
    }
    parse_types = {
        path: decimals.get(column_name(path, naming), arrow_type)
        for path, arrow_type in types.items()
    }
    options = pa_json.ParseOptions(unexpected_field_behavior="infer")
    errors: List[MessageDecodeError] = []
    new_types: TArrowTypes = {}

    def explicit_types(table: pa.Table) -> TArrowTypes:
        """Types of the inferred fields that must parse with another type"""
        explicit: TArrowTypes = {}
        for path, arrow_type in scalar_types(table.schema).items():
            name = column_name(path, naming)
            if name in decimals and arrow_type != decimals[name]:
                explicit[path] = decimals[name]
            elif path not in parse_types and pa.types.is_timestamp(arrow_type):
                # inferred from the first values, e.g. whole seconds without a time zone
                explicit[path] = UTC_MICROS
        return explicit

    def read(data: bytes) -> pa.Table:
        options.explicit_schema = nested_schema(parse_types) if parse_types else None
        return pa_json.read_json(
            io.BytesIO(data),
            read_options=pa_json.ReadOptions(
                block_size=max(len(data) + 1, MIN_BLOCK_SIZE)
            ),
            parse_options=options,
        )

    def convert(batch: List[Tuple[Message, Optional[str]]]) -> pa.Table:
        values = [
            b"{}" if msg.value() is None else payload(msg.value()) for msg, _ in batch
        ]
        data = b"\n".join(values)
        options.newlines_in_values = any(b"\n" in value for value in values)
        table = read(data)
        explicit = explicit_types(table)
        if explicit:
            parse_types.update(explicit)
            table = read(data)
        if table.num_rows != len(batch):
            raise ValueError("not a JSON object")
        if allowed is not None and not set(table.column_names) <= allowed:
            raise ValueError(
                f"fields {sorted(set(table.column_names) - allowed)} are not in its schema"
            )
        new_types.update(
            (path, arrow_type)
            for path, arrow_type in scalar_types(table.schema).items()
            if path not in types
        )
        table = flatten_structs(table, naming, json_columns)
        if fields:
            table = convert_arrays(table, fields, naming)
        keys = [key for _, key in batch]
        for name, array in metadata_columns(
            [m for m, _ in batch], keys, naming
        ).items():
            table = table.append_column(name, array)
        return cast_to_columns(table, columns, naming)

    def convert_all(batch: List[Tuple[Message, Optional[str]]]) -> List[pa.Table]:
        try:
            return [convert(batch)]
        except (pa.ArrowException, ValueError) as ex:
            if len(batch) == 1:
                errors.append(MessageDecodeError(batch[0][0], str(ex)))
                return []
            middle = len(batch) // 2
            return convert_all(batch[:middle]) + convert_all(batch[middle:])

    keyed: List[Tuple[Message, Optional[str]]] = []
    for msg in messages:
        try:
            keyed.append((msg, text_key(msg)))
        except MessageDecodeError as ex:
            errors.append(ex)
    tables = convert_all(keyed) if keyed else []
    for path, arrow_type in new_types.items():
        types.setdefault(path, arrow_type)
    for error in sorted(errors, key=lambda e: (e.partition, e.offset)):
        on_error(error)
    if not tables:
        return None
    if len(tables) == 1:
        return tables[0]
    return pa.concat_tables(tables, promote_options="permissive")


def avro_arrow_types(fields: TFields) -> TArrowTypes:
    """Arrow types of decoded Avro values by their paths"""
    caps = destination_caps()
    return {
        path: (
            pa.string()
            if field.column["data_type"] == "json"
            else get_py_arrow_datatype(field.column, caps, "UTC")
        )
        for path, field in fields.items()
    }


def avro_arrow_table(
    rows: List[Dict[str, Any]],
    messages: List[Message],
    keys: Optional[List[Optional[str]]],
    types: TArrowTypes,
    columns: Optional[TTableSchemaColumns],
    naming: NamingConvention,
) -> pa.Table:
    """Arrow table of decoded Avro values.

    Args:
        rows: Values of `messages` with nested records, json columns as JSON strings.
        messages: Messages of the rows.
        keys: Text keys of the messages, `None` when the rows hold decoded keys.
        types: Arrow types of the leaves of the rows by their paths.
        columns: Columns of the table, cast after the rows are flattened.
        naming: Naming convention of the destination schema.
    """
    table: Optional[pa.Table] = None
    if types:
        table = flatten_structs(
            pa.Table.from_pylist(rows, schema=nested_schema(types)), naming, set()
        )
    for name, array in metadata_columns(messages, keys, naming).items():
        table = (
            table.append_column(name, array)
            if table is not None
            else pa.table({name: array})
        )
    return cast_to_columns(table, columns, naming)
