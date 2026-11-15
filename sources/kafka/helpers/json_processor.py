import json as stdlib_json
from functools import partial
from typing import Any, Dict, Hashable, List, Literal, Optional, Sequence, Set, Tuple

import dlt
from confluent_kafka import Message
from dlt.common import Decimal, json
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TDataItem

from .columns import TFields, TPath, convert_row, row_conversions
from .json_schema import json_schema_fields, json_schema_name, top_level_fields
from .kafka_connect import connect_fields, connect_payload, connect_schema
from .processors import (
    MessageDecodeError,
    SchemaProcessor,
    TBackend,
    TOnDecodeError,
    TPayload,
    TSchemaTableName,
    default_schema_table_name,
    message_id,
    message_metadata,
    text_key,
)
from .registry import (
    VALUE_SCHEMA_ID_HEADER,
    TSchemaId,
    body_start,
    framed_schema_id,
    latest_registry_schema,
    registry_schema,
)

TEnvelope = Literal["kafka_connect"]
KAFKA_CONNECT_ID = "kafka_connect"


def value_body(start: int, value: bytes) -> bytes:
    return value[start:]


def envelope_payload(value: bytes) -> bytes:
    """The payload of a Kafka Connect envelope, values without one are used as they are"""
    payload = connect_payload(value)
    if payload is None:
        return value
    return b"{}" if payload == b"null" else payload


def json_value(msg: Message, payload: TPayload, use_decimal: bool) -> Dict[str, Any]:
    """The JSON object in the `payload` of the value of `msg`, `{}` for a tombstone"""
    value = msg.value()
    if value is None:
        return {}
    raw = payload(value)
    try:
        if use_decimal:
            data = stdlib_json.loads(raw, parse_float=Decimal)
        else:
            data = json.loadb(raw)
    except ValueError:
        # integers beyond 64 bits
        try:
            data = stdlib_json.loads(raw)
        except ValueError as ex:
            raise MessageDecodeError(msg, f"not valid JSON ({ex})")
    if not isinstance(data, dict):
        raise MessageDecodeError(msg, "the value is not a JSON object")
    return data


class JsonProcessor(SchemaProcessor):
    """Loads values that are JSON objects with their fields as top-level columns.

    Values written with a schema in the Schema Registry, with the Confluent framing in the
    payload or a schema id header, are typed with the columns of their JSON Schema. Other values
    are typed by dlt. Metadata goes to the `_kafka__*` columns and `_kafka_msg_id`, like with
    `default_msg_processor`.

    Args:
        backend: "python" yields dicts, "pyarrow" Arrow tables. "auto" picks pyarrow when it is
            installed.
        strict: Every topic needs a schema in the Schema Registry, every message a schema id and no
            fields outside its schema.
        on_decode_error: "raise" fails the run at a message that does not decode, "skip" logs it
            and loads the other messages.
        table_per_schema: Loads the messages of every schema into a table of its own, for example
            of topics with several record types.
        table_name: Names the table of a schema with `table_per_schema`, called with the topic and
            the name of the schema: its `title`, else its subject. `<topic>__<name>` by default.
        envelope: "kafka_connect" reads values written by the Kafka Connect `JsonConverter` with
            `schemas.enable=true`: the payload is loaded and typed with the schema next to it.
    """

    def __init__(
        self,
        backend: TBackend = "auto",
        strict: bool = False,
        on_decode_error: TOnDecodeError = "raise",
        table_per_schema: bool = False,
        table_name: TSchemaTableName = default_schema_table_name,
        envelope: Optional[TEnvelope] = None,
    ) -> None:
        super().__init__(backend, on_decode_error, table_per_schema, table_name)
        self.strict = strict
        self.envelope = envelope
        # schema ids of other schema types, their messages are decode errors
        self._other_types: Dict[Hashable, str] = {}
        self._arrow_types: Dict[str, Dict[TPath, Any]] = {}
        # tombstones have no envelope, they reuse the Kafka Connect fields of their table
        self._table_connect: Dict[str, TFields] = {}

    def prepare(
        self,
        topics: Sequence[str],
        registry: Optional[Any],
        naming: NamingConvention,
    ) -> None:
        super().prepare(topics, registry, naming)
        if not self.strict or self.envelope:
            return
        if registry is None:
            raise ValueError("JsonProcessor with `strict=True` needs a Schema Registry")
        if not self.table_per_schema:
            missing = [t for t in topics if self.table_schema(t, None)[1] is None]
            if missing:
                raise ValueError(
                    f"Topics {missing} have no schema in the Schema Registry, the subject"
                    " `<topic>-value` does not exist"
                )

    def schema_id(self, msg: Message) -> Optional[Hashable]:
        value = msg.value()
        if value is None:
            return None
        if self.envelope:
            schema = connect_schema(value)
            return None if schema is None else (KAFKA_CONNECT_ID, schema)
        # JSON never starts with a 0 byte
        return framed_schema_id(value, msg.headers(), VALUE_SCHEMA_ID_HEADER)

    def compute_table_fields(
        self, topic: str, schema_id: Optional[Hashable]
    ) -> Tuple[str, Optional[TFields]]:
        sid: Optional[TSchemaId] = schema_id  # type: ignore[assignment]
        if sid is not None and sid[0] == KAFKA_CONNECT_ID:
            schema = json.loadb(sid[1])
            table = topic
            if self.table_per_schema and schema.get("name"):
                table = self.table_name(topic, schema["name"])
            return table, connect_fields(schema)
        if self.registry is None:
            return topic, None
        if sid is None:
            latest = latest_registry_schema(self.registry, f"{topic}-value")
            schema = None if latest is None else latest.schema
        else:
            schema = registry_schema(self.registry, sid)
        if schema is None:
            return topic, None
        schema_type = schema.schema_type or "AVRO"
        if schema_type != "JSON" and sid is not None:
            self._other_types[sid] = schema_type
            return topic, None
        if schema_type != "JSON":
            raise ValueError(
                f"Topic `{topic}` has a {schema_type} schema in the Schema Registry,"
                " JsonProcessor reads JSON values only"
            )
        table = topic
        if self.table_per_schema and sid is not None:
            table = self.table_name(topic, json_schema_name(self.registry, sid, schema))
        return table, json_schema_fields(json.loads(schema.schema_str))

    def __call__(
        self,
        table: str,
        schema_id: Optional[Hashable],
        columns: Optional[TTableSchemaColumns],
        messages: List[Message],
    ) -> List[TDataItem]:
        topic = messages[0].topic()
        if schema_id in self._other_types:
            reason = f"its schema is {self._other_types[schema_id]}, JsonProcessor reads JSON only"
            for msg in messages:
                self.decode_error(MessageDecodeError(msg, reason))
            return []
        _, fields, _ = self.table_fields(topic, schema_id)
        if self.strict and not self.envelope and (schema_id is None or fields is None):
            # tombstones have no schema
            reason = (
                "it has no schema id"
                if schema_id is None
                else "its schema is not found"
            )
            tombstones = []
            for msg in messages:
                if msg.value() is None:
                    tombstones.append(msg)
                else:
                    self.decode_error(MessageDecodeError(msg, reason))
            messages = tombstones
            if not messages:
                return []
        connect: TFields = {}
        if self.envelope:
            connect = fields or self._table_connect.get(table, {})
            self._table_connect[table] = connect
        payload: TPayload = envelope_payload
        if not self.envelope:
            start = 0 if schema_id is None else body_start(schema_id)  # type: ignore[arg-type]
            payload = partial(value_body, start)
        allowed = top_level_fields(fields) if self.strict and fields else None
        if self.backend == "pyarrow":
            from .arrow import connect_types, json_arrow_table, schema_types

            types = self._arrow_types.setdefault(table, {})
            types.update(
                connect_types(connect) if connect else schema_types(fields or {})
            )
            item = json_arrow_table(
                messages,
                payload,
                types,
                columns,
                allowed,
                self.naming,
                self.decode_error,
                connect or fields or {},
            )
            if item is None:
                return []
        else:
            item = self.rows(
                messages, payload, columns, allowed, connect or fields or {}
            )
            if not item:
                return []
        return [dlt.mark.with_table_name(item, table)]

    def rows(
        self,
        messages: List[Message],
        payload: TPayload,
        columns: Optional[TTableSchemaColumns],
        allowed: Optional[Set[str]],
        fields: TFields,
    ) -> List[Dict[str, Any]]:
        conversions = row_conversions(fields, python_rows=True)
        use_decimal = any(
            column.get("data_type") == "decimal" for column in (columns or {}).values()
        )
        rows = []
        for msg in messages:
            try:
                key = text_key(msg)
                value = json_value(msg, payload, use_decimal)
                if allowed is not None and not value.keys() <= allowed:
                    raise MessageDecodeError(
                        msg,
                        f"fields {sorted(value.keys() - allowed)} are not in its schema",
                    )
                convert_row(value, conversions)
            except (MessageDecodeError, ValueError, TypeError, ArithmeticError) as ex:
                if not isinstance(ex, MessageDecodeError):
                    ex = MessageDecodeError(msg, str(ex))
                self.decode_error(ex)
                continue
            value["_kafka"] = message_metadata(msg, key)
            value["_kafka_msg_id"] = message_id(msg, key)
            rows.append(value)
        return rows
