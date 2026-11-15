import importlib.util
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import dlt
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Message
from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TDataItem
from dlt.common.utils import digest128

from .columns import EPOCH

TMessageProcessor = Callable[[Message], Dict[str, Any]]
TPayload = Callable[[bytes], bytes]
"""Returns the encoded value inside a message value"""
TBackend = Literal["auto", "python", "pyarrow"]
TOnDecodeError = Literal["raise", "skip"]
TSchemaTableName = Callable[[str, str], str]


def decode_utf8(value: Optional[bytes]) -> Union[str, bytes, None]:
    """Decodes `value` as UTF-8 text, keeps bytes that are not valid UTF-8"""
    if value is None:
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value


def message_timestamp(msg: Message) -> Optional[datetime]:
    """Timestamp of `msg` in UTC, `None` if the broker does not keep timestamps"""
    ts_type, ts_ms = msg.timestamp()
    if ts_type == TIMESTAMP_NOT_AVAILABLE:
        return None
    return EPOCH + timedelta(milliseconds=ts_ms)


def message_metadata(msg: Message, key: Union[str, bytes, None]) -> Dict[str, Any]:
    """The `_kafka` dict of `msg` without its data"""
    return {
        "partition": msg.partition(),
        "topic": msg.topic(),
        "key": key,
        "offset": msg.offset(),
        "ts": {
            "type": msg.timestamp()[0],
            "value": message_timestamp(msg),
        },
    }


def message_id(msg: Message, key: Union[str, bytes, None]) -> str:
    """Hash of topic, partition and key, shared by the messages of a key"""
    return digest128(msg.topic() + str(msg.partition()) + str(key))


def default_msg_processor(msg: Message) -> Dict[str, Any]:
    """Basic Kafka message processor.

    Returns the message value and metadata. Key and value are UTF-8 text, or bytes when they are not
    valid UTF-8; a tombstone has `None` as value. Timestamp consists of two values: type and value.
    Type is one of the Kafka constants:
        TIMESTAMP_NOT_AVAILABLE - Timestamps not supported by broker, value is `None`.
        TIMESTAMP_CREATE_TIME - Message creation time (or source / producer time).
        TIMESTAMP_LOG_APPEND_TIME - Broker receive time.

    `_kafka_msg_id` is a hash of topic, partition and key, so messages with the same key share it.

    Args:
        msg (confluent_kafka.Message): A single Kafka message.

    Returns:
        dict: Processed Kafka message.
    """
    key = decode_utf8(msg.key())
    metadata = message_metadata(msg, key)
    metadata["data"] = decode_utf8(msg.value())
    return {"_kafka": metadata, "_kafka_msg_id": message_id(msg, key)}


class MessageDecodeError(ValueError):
    def __init__(self, msg: Message, reason: str) -> None:
        self.topic = msg.topic()
        self.partition = msg.partition()
        self.offset = msg.offset()
        value = msg.value() or b""
        super().__init__(
            f"dlt cannot decode the message at offset {self.offset} of topic `{self.topic}`"
            f" partition {self.partition}: {reason}. The value starts with"
            f" `{value[:16].hex(' ')}`"
        )


def default_schema_table_name(topic: str, schema: str) -> str:
    return f"{topic}__{schema}"


def resolve_backend(backend: TBackend, processor: str) -> Literal["python", "pyarrow"]:
    """Picks pyarrow for "auto" when it is installed, without importing it"""
    has_pyarrow = importlib.util.find_spec("pyarrow") is not None
    if backend == "auto":
        return "pyarrow" if has_pyarrow else "python"
    if backend == "pyarrow" and not has_pyarrow:
        raise MissingDependencyException(
            f"{processor} with the pyarrow backend", ["pyarrow"]
        )
    return backend


def text_key(msg: Message) -> Optional[str]:
    key = msg.key()
    if key is None:
        return None
    try:
        return key.decode("utf-8")
    except UnicodeDecodeError:
        raise MessageDecodeError(msg, "the key is not UTF-8 text")


class BatchProcessor(ABC):
    """Converts messages of one topic and one schema into data items.

    `kafka_consumer` groups the messages of every consumed batch by topic and `schema_id`, emits the
    columns `table_schema` returns as table hints and calls the processor with each group.
    """

    registry: Optional[Any] = None
    naming: Optional[NamingConvention] = None

    def prepare(
        self,
        topics: Sequence[str],
        registry: Optional[Any],
        naming: NamingConvention,
    ) -> None:
        """Called before the first message.

        Args:
            topics: The topics to read.
            registry: A `confluent_kafka.schema_registry.SchemaRegistryClient`, or `None`.
            naming: Naming convention of the destination schema.
        """
        self.registry = registry
        self.naming = naming

    def schema_id(self, msg: Message) -> Optional[Hashable]:
        """Id of the schema `msg` was written with, `None` when it has none"""
        return None

    def table_schema(
        self, topic: str, schema_id: Optional[Hashable]
    ) -> Tuple[str, Optional[TTableSchemaColumns]]:
        """Table of the messages with `schema_id` and the columns of their schema.

        With `schema_id` `None` before the first message, it returns the columns known for `topic`.
        The columns are `None` when there is no schema, no table hints are emitted then.
        """
        return topic, None

    @abstractmethod
    def __call__(
        self,
        table: str,
        schema_id: Optional[Hashable],
        columns: Optional[TTableSchemaColumns],
        messages: List[Message],
    ) -> List[TDataItem]:
        """Converts `messages` into data items marked with their table.

        Args:
            table: The table from `table_schema`.
            schema_id: The schema id of the messages.
            columns: The emitted columns of `table` merged with the columns the user applied to the
                resource, `None` when neither exists.
            messages: Messages of one topic and one schema id.
        """


class SchemaProcessor(BatchProcessor, ABC):
    """A processor that types columns with the schemas of the messages, with the options and the
    table cache that `JsonProcessor` and `AvroProcessor` share."""

    def __init__(
        self,
        backend: TBackend,
        on_decode_error: TOnDecodeError,
        table_per_schema: bool,
        table_name: TSchemaTableName,
    ) -> None:
        self.backend = resolve_backend(backend, type(self).__name__)
        self.on_decode_error = on_decode_error
        self.table_per_schema = table_per_schema
        self.table_name = table_name
        self._tables: Dict[
            Tuple[str, Optional[Hashable]], Tuple[str, Optional[TTableSchemaColumns]]
        ] = {}

    def table_schema(
        self, topic: str, schema_id: Optional[Hashable]
    ) -> Tuple[str, Optional[TTableSchemaColumns]]:
        key = (topic, schema_id)
        if key not in self._tables:
            self._tables[key] = self.compute_table_schema(topic, schema_id)
        return self._tables[key]

    @abstractmethod
    def compute_table_schema(
        self, topic: str, schema_id: Optional[Hashable]
    ) -> Tuple[str, Optional[TTableSchemaColumns]]:
        """`table_schema` before it is cached"""

    def decode_error(self, error: MessageDecodeError) -> None:
        if self.on_decode_error == "raise":
            raise error
        logger.warning(f"Skipped: {error}")


class MessageFunctionProcessor(BatchProcessor):
    """Calls a message processor function for every message. `_kafka.topic` names the table."""

    def __init__(self, function: TMessageProcessor) -> None:
        self.function = function

    def __call__(
        self,
        table: str,
        schema_id: Optional[Hashable],
        columns: Optional[TTableSchemaColumns],
        messages: List[Message],
    ) -> List[TDataItem]:
        tables: Dict[str, List[Dict[str, Any]]] = {}
        for msg in messages:
            row = self.function(msg)
            tables.setdefault(row["_kafka"]["topic"], []).append(row)
        return [dlt.mark.with_table_name(rows, name) for name, rows in tables.items()]
