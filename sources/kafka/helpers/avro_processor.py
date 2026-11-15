import importlib.util
import io
from typing import (
    Any,
    Dict,
    Hashable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import dlt
from confluent_kafka import Message
from dlt.common import json, logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TDataItem

from .avro import (
    avro_fields,
    decode_schema,
    is_record,
    record_name,
    union_fields,
)
from .columns import (
    TFields,
    TPath,
    check_names,
    convert_row,
    drop_null_records,
    fields_columns,
    record_paths,
    row_conversions,
)
from .processors import (
    MessageDecodeError,
    SchemaProcessor,
    TBackend,
    TOnDecodeError,
    TSchemaTableName,
    default_schema_table_name,
    message_id,
    message_metadata,
    text_key,
)
from .registry import (
    KEY_SCHEMA_ID_HEADER,
    VALUE_SCHEMA_ID_HEADER,
    TSchemaId,
    body_start,
    framed_schema_id,
    latest_registry_schema,
    referenced_schemas,
    registry_schema,
)

KEY_PREFIX: TPath = ("_kafka", "key")
# fastavro loses the named types of a parsed union when it parses it again to decode a value,
# so a union decodes as a record with the union as its only field
UNION_ROOT = "dlt_union_root"
DATA_CONTRACT_RULES = ("domain_rules", "migration_rules", "encoding_rules")


class AvroSchemaId(NamedTuple):
    """Schemas of a message: of its value, the branch of a value schema that is a union, of its key"""

    value: Optional[TSchemaId]
    branch: Optional[int]
    key: Optional[TSchemaId]


class WriterSchema(NamedTuple):
    """A schema from the Schema Registry, parsed by fastavro"""

    schema: Any
    named: Dict[str, Any]
    """Named types of the schema and its references by full name"""
    decoder: Any
    """Schema that decodes values, a record that holds a union"""

    @property
    def branches(self) -> Optional[List[Any]]:
        """Branches of a schema that is a union"""
        return self.schema if isinstance(self.schema, list) else None


class AvroProcessor(SchemaProcessor):
    """Loads Avro values written with a schema in the Schema Registry, with their fields as columns.

    Values carry the Confluent framing: the id of their schema in a 5-byte prefix or in a
    `__value_schema_id` header. Columns are typed by the schema. Keys with the framing and an Avro
    schema load into `_kafka__key`, flattened when the key is a record. Other keys load as text.
    Metadata goes to the `_kafka__*` columns and `_kafka_msg_id`, like with `default_msg_processor`.

    Args:
        backend: "python" yields dicts, "pyarrow" Arrow tables. "auto" picks pyarrow when it is
            installed.
        on_decode_error: "raise" fails the run at a message that does not decode, "skip" logs it
            and loads the other messages.
        table_per_schema: Loads the messages of every schema into a table of its own, for example
            of topics with several record types.
        table_name: Names the table of a schema with `table_per_schema`, called with the topic and
            the full name of the record. `<topic>__<name>` by default.
    """

    def __init__(
        self,
        backend: TBackend = "auto",
        on_decode_error: TOnDecodeError = "raise",
        table_per_schema: bool = False,
        table_name: TSchemaTableName = default_schema_table_name,
    ) -> None:
        if importlib.util.find_spec("fastavro") is None:
            raise MissingDependencyException("AvroProcessor", ["fastavro"])
        super().__init__(backend, on_decode_error, table_per_schema, table_name)
        self._writers: Dict[TSchemaId, Union[WriterSchema, str]] = {}
        self._fields: Dict[Tuple[str, Optional[Hashable]], TFields] = {}

    def prepare(
        self,
        topics: Sequence[str],
        registry: Optional[Any],
        naming: NamingConvention,
    ) -> None:
        if registry is None:
            raise ValueError(
                "AvroProcessor needs a Schema Registry. Pass `schema_registry` to"
                " `kafka_consumer`."
            )
        super().prepare(topics, registry, naming)

    def schema_id(self, msg: Message) -> Optional[Hashable]:
        value = msg.value()
        value_id = (
            None
            if value is None
            else framed_schema_id(value, msg.headers(), VALUE_SCHEMA_ID_HEADER)
        )
        branch = None
        if value_id is not None:
            writer = self.writer(value_id)
            if isinstance(writer, WriterSchema) and writer.branches is not None:
                branch = _union_index(value, body_start(value_id))
        key_id = self._key_id(msg)
        if value_id is None and key_id is None:
            return None
        return AvroSchemaId(value_id, branch, key_id)

    def compute_table_schema(
        self, topic: str, schema_id: Optional[Hashable]
    ) -> Tuple[str, Optional[TTableSchemaColumns]]:
        avro_id: Optional[AvroSchemaId] = schema_id  # type: ignore[assignment]
        fields = self.fields(topic, avro_id)
        table = topic
        value = self._value_writer(topic, avro_id)
        if self.table_per_schema and isinstance(value, WriterSchema):
            if value.branches is None:
                name = record_name(value.schema, value.named)
            elif avro_id is not None and _is_branch(avro_id.branch, value):
                name = record_name(value.schema[avro_id.branch], value.named)
            else:
                # the branch is known only from a message
                return topic, None
            if name:
                table = self.table_name(topic, name)
        return table, fields_columns(fields, self.naming) or None

    def fields(self, topic: str, schema_id: Optional[AvroSchemaId]) -> TFields:
        """Fields of the values and keys of a schema id, of the latest schemas of `topic` for
        `None`"""
        cache_key = (topic, schema_id)
        if cache_key in self._fields:
            return self._fields[cache_key]
        fields: TFields = {}
        value = self._value_writer(topic, schema_id)
        if isinstance(value, WriterSchema):
            if value.branches is None:
                fields.update(avro_fields(value.schema, value.named))
            elif (
                self.table_per_schema
                and schema_id is not None
                and _is_branch(schema_id.branch, value)
            ):
                fields.update(avro_fields(value.schema[schema_id.branch], value.named))
            else:
                fields.update(union_fields(value.schema, value.named))
        key = self._key_writer(topic, schema_id)
        if isinstance(key, WriterSchema):
            fields.update(
                avro_fields(key.schema, key.named, prefix=KEY_PREFIX, scalar_name=None)
            )
        check_names(fields, self.naming)
        self._fields[cache_key] = fields
        return fields

    def writer(self, schema_id: TSchemaId) -> Union[WriterSchema, str]:
        """Parsed schema of `schema_id`, or why there is none"""
        if schema_id not in self._writers:
            schema = registry_schema(self.registry, schema_id)
            self._writers[schema_id] = (
                f"schema {schema_id[1]} is not in the Schema Registry"
                if schema is None
                else self._load(schema, str(schema_id[1]))
            )
        return self._writers[schema_id]

    def _load(self, schema: Any, label: str) -> Union[WriterSchema, str]:
        import fastavro

        schema_type = schema.schema_type or "AVRO"
        if schema_type != "AVRO":
            return f"schema {label} is a {schema_type} schema, AvroProcessor reads Avro only"
        named: Dict[str, Any] = {}
        try:
            for reference in referenced_schemas(self.registry, schema):
                fastavro.parse_schema(decode_schema(reference), named)
            root = decode_schema(json.loads(schema.schema_str))
            decoder: Any
            if isinstance(root, list):
                decoder = fastavro.parse_schema(
                    {
                        "type": "record",
                        "name": UNION_ROOT,
                        "fields": [{"name": "value", "type": root}],
                    },
                    named,
                )
                parsed = decoder["fields"][0]["type"]
            else:
                decoder = parsed = fastavro.parse_schema(root, named)
        except Exception as ex:
            return (
                f"schema {label} is not a valid Avro schema ({type(ex).__name__}: {ex})"
            )
        rules = getattr(schema, "rule_set", None)
        if rules is not None and any(
            getattr(rules, kind, None) for kind in DATA_CONTRACT_RULES
        ):
            logger.warning(
                f"Schema {label} has data contract rules that AvroProcessor does not apply:"
                " encrypted fields load as ciphertext, transforms and migrations are skipped"
            )
        return WriterSchema(parsed, named, decoder)

    def _value_writer(
        self, topic: str, schema_id: Optional[AvroSchemaId]
    ) -> Union[WriterSchema, str, None]:
        if schema_id is None:
            return self._latest(f"{topic}-value")
        if schema_id.value is None:
            return None
        return self.writer(schema_id.value)

    def _key_writer(
        self, topic: str, schema_id: Optional[AvroSchemaId]
    ) -> Union[WriterSchema, str, None]:
        if schema_id is None:
            return self._latest(f"{topic}-key")
        if schema_id.key is None:
            return None
        return self.writer(schema_id.key)

    def _latest(self, subject: str) -> Union[WriterSchema, str, None]:
        registered = latest_registry_schema(self.registry, subject)
        if registered is None:
            return None
        schema_id: TSchemaId = ("id", registered.schema_id)
        if schema_id not in self._writers:
            self._writers[schema_id] = self._load(
                registered.schema, str(registered.schema_id)
            )
        return self._writers[schema_id]

    def _key_id(self, msg: Message) -> Optional[TSchemaId]:
        """Schema id of a key with the framing and an Avro schema that decodes all of it"""
        key = msg.key()
        if key is None:
            return None
        key_id = framed_schema_id(key, msg.headers(), KEY_SCHEMA_ID_HEADER)
        if key_id is None:
            return None
        # a long key that starts with a 0 byte makes an id without an Avro schema
        writer = self.writer(key_id)
        if not isinstance(writer, WriterSchema):
            return None
        try:
            decode(writer, key, body_start(key_id))
        except Exception:
            return None
        return key_id

    def __call__(
        self,
        table: str,
        schema_id: Optional[Hashable],
        columns: Optional[TTableSchemaColumns],
        messages: List[Message],
    ) -> List[TDataItem]:
        avro_id: Optional[AvroSchemaId] = schema_id  # type: ignore[assignment]
        topic = messages[0].topic()
        fields = self.fields(topic, avro_id) if avro_id is not None else {}
        value_writer = self._value_writer(topic, avro_id) if avro_id else None
        key_writer = self._key_writer(topic, avro_id) if avro_id else None
        scalar_value = isinstance(value_writer, WriterSchema) and not is_record(
            _branch_schema(value_writer, avro_id.branch if avro_id else None),
            value_writer.named,
        )
        conversions = row_conversions(fields, self.backend == "python")
        null_records = record_paths(fields)
        rows: List[Dict[str, Any]] = []
        decoded: List[Message] = []
        keys: List[Any] = []
        for msg in messages:
            try:
                if isinstance(key_writer, WriterSchema):
                    key: Any = decode(key_writer, msg.key(), body_start(avro_id.key))
                    id_key: Any = msg.key()
                else:
                    key = id_key = text_key(msg)
                row = self._decode_value(msg, avro_id, value_writer, scalar_value)
                if self.backend == "python":
                    row["_kafka"] = message_metadata(msg, key)
                    row["_kafka_msg_id"] = message_id(msg, id_key)
                    drop_null_records(row, null_records)
                elif isinstance(key_writer, WriterSchema):
                    row["_kafka"] = {"key": key}
                convert_row(row, conversions)
            except MessageDecodeError as ex:
                self.decode_error(ex)
                continue
            except Exception as ex:
                self.decode_error(MessageDecodeError(msg, f"{type(ex).__name__}: {ex}"))
                continue
            rows.append(row)
            decoded.append(msg)
            keys.append(key)
        if not rows:
            return []
        if self.backend == "pyarrow":
            from .arrow import avro_arrow_table, avro_arrow_types

            item: TDataItem = avro_arrow_table(
                rows,
                decoded,
                None if isinstance(key_writer, WriterSchema) else keys,
                avro_arrow_types(fields),
                columns,
                self.naming,
            )
        else:
            item = rows
        return [dlt.mark.with_table_name(item, table)]

    def _decode_value(
        self,
        msg: Message,
        schema_id: Optional[AvroSchemaId],
        writer: Union[WriterSchema, str, None],
        scalar: bool,
    ) -> Dict[str, Any]:
        value = msg.value()
        if value is None:
            return {}
        if schema_id is None or schema_id.value is None:
            raise MessageDecodeError(
                msg, "it has no schema id: no Confluent prefix and no schema id header"
            )
        if not isinstance(writer, WriterSchema):
            raise MessageDecodeError(msg, str(writer))
        try:
            data = decode(writer, value, body_start(schema_id.value))
        except Exception as ex:
            raise MessageDecodeError(
                msg,
                f"it is not valid Avro of schema {schema_id.value[1]}"
                f" ({type(ex).__name__}: {ex})",
            )
        if scalar or not isinstance(data, dict):
            return {"value": data}
        return data


def decode(writer: WriterSchema, data: bytes, start: int) -> Any:
    """Decodes `data` from `start` to its end with `writer`. fastavro ignores bytes left after a
    value, so they are checked here: a value written with another schema leaves them."""
    from fastavro import schemaless_reader

    # BytesIO shares the bytes of `data`, a slice copies them
    buffer = io.BytesIO(data)
    buffer.seek(start)
    value: Any = schemaless_reader(buffer, writer.decoder, None)
    if writer.branches is not None:
        value = value["value"]
    left = len(data) - buffer.tell()
    if left:
        raise ValueError(f"{left} bytes are left after the value")
    return value


def _union_index(data: bytes, start: int) -> Optional[int]:
    """Branch of a union value, the zigzag varint it starts with"""
    result = shift = 0
    for byte in data[start : start + 10]:
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return (result >> 1) ^ -(result & 1)
        shift += 7
    return None


def _is_branch(branch: Optional[int], writer: WriterSchema) -> bool:
    return branch is not None and 0 <= branch < len(writer.branches or [])


def _branch_schema(writer: WriterSchema, branch: Optional[int]) -> Any:
    """Schema of the values of `branch`, the whole schema when it is not a union"""
    if writer.branches is None or not _is_branch(branch, writer):
        return writer.schema
    return writer.schema[branch]
