"""A source to extract Kafka messages.

When extraction starts, the end offset of every partition is read and
messages are read only up to it, instead of waiting for new messages in
an endless loop like a Kafka consumer does.
"""

from contextlib import closing
from datetime import timezone
from typing import Dict, Hashable, Iterable, List, Optional, Tuple, Union

import dlt
from confluent_kafka import Consumer, Message
from dlt.common.exceptions import DltException
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import TAnyDateTime, TDataItem
from dlt.extract.items import DataItemWithMeta
from dlt.extract.resource import DltResource

from .helpers.avro_processor import AvroProcessor
from .helpers.client import group_offsets, partition_ranges, read_batches
from .helpers.credentials import (
    KafkaCredentials,
    KafkaMskIamCredentials,
    SchemaRegistryCredentials,
)
from .helpers.hints import TableHints
from .helpers.json_processor import JsonProcessor
from .helpers.processors import (
    BatchProcessor,
    MessageDecodeError,
    MessageFunctionProcessor,
    TMessageProcessor,
    default_msg_processor,
)

__all__ = [
    "kafka_consumer",
    "KafkaCredentials",
    "KafkaMskIamCredentials",
    "SchemaRegistryCredentials",
    "BatchProcessor",
    "AvroProcessor",
    "JsonProcessor",
    "MessageDecodeError",
    "default_msg_processor",
]


@dlt.resource(name="kafka_messages", standalone=True)
def kafka_consumer(
    topics: Union[str, List[str]],
    credentials: Union[
        KafkaMskIamCredentials, KafkaCredentials, Consumer
    ] = dlt.secrets.value,
    msg_processor: Union[BatchProcessor, TMessageProcessor] = default_msg_processor,
    batch_size: int = 3000,
    batch_timeout: float = 3,
    start_from: Optional[TAnyDateTime] = None,
    end_time: Optional[TAnyDateTime] = None,
    request_timeout: float = 30,
    schema_registry: Optional[SchemaRegistryCredentials] = None,
) -> Iterable[TDataItem]:
    """Extract recent messages from the given Kafka topics.

    The resource keeps the last read offset of every partition in its state, and reads the
    messages after it on the next run. Messages from different topics are saved in different
    tables.

    Args:
        topics (Union[str, List[str]]): Names of topics to extract.
        credentials (Union[KafkaMskIamCredentials, KafkaCredentials, Consumer]): Auth
            credentials or an initiated Kafka consumer. By default, dlt takes them from secrets,
            as MSK IAM credentials when `aws_region` is set. Set `enable.partition.eof` in a
            passed consumer, so the read ends when all partitions are read.
        msg_processor (Union[BatchProcessor, Callable]): Converts the messages before they are
            loaded. A function is called with every message and returns a dict, whose
            `_kafka.topic` names the table. A `BatchProcessor`, for example `JsonProcessor`,
            converts batches of messages of one topic and one schema.
        batch_size (int): Messages batch size to read at once.
        batch_timeout (float): Maximum time to wait for a batch consume, in seconds. The read
            stops when no message arrives in this time.
        start_from (Optional[TAnyDateTime]): A timestamp, at which to start reading. Older
            messages are ignored. Partitions with a stored offset continue after it, when it is
            later.
        end_time (Optional[TAnyDateTime]): A timestamp, at which to stop reading. Newer messages
            are ignored. With `end_time` the stored offsets are neither used nor updated, so a
            window of messages can be loaded without changing the incremental state.
        request_timeout (float): Timeout of the requests for topic metadata and offsets, in
            seconds.
        schema_registry (Optional[SchemaRegistryCredentials]): A Schema Registry, by default taken
            from `schema_registry` in the source config. `JsonProcessor` types the columns with the
            schemas of the messages.

    Yields:
        Iterable[TDataItem]: Kafka messages.
    """
    if not isinstance(topics, list):
        topics = [topics]

    if isinstance(credentials, Consumer):
        consumer = credentials
    elif isinstance(credentials, KafkaCredentials):
        consumer = credentials.init_consumer()
    else:
        raise TypeError(
            "Wrong credentials type provided. Need to be of type: "
            "KafkaCredentials or confluent_kafka.Consumer"
        )

    start_dt = (
        ensure_datetime_in_tz(start_from, timezone.utc)
        if start_from is not None
        else None
    )
    end_dt = (
        ensure_datetime_in_tz(end_time, timezone.utc) if end_time is not None else None
    )
    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        raise ValueError("`start_from` must be before `end_time`")

    if end_dt is None:
        offsets = dlt.current.resource_state().setdefault("offsets", {})
    else:
        offsets = {}

    processor = (
        msg_processor
        if isinstance(msg_processor, BatchProcessor)
        else MessageFunctionProcessor(msg_processor)
    )
    registry = schema_registry.init_client() if schema_registry is not None else None
    resource = _current_resource()
    table_override = _static_table_name(resource)
    hints = TableHints(resource)

    with closing(consumer):
        processor.prepare(topics, registry, _naming())
        for topic in topics:
            table, columns = processor.table_schema(topic, None)
            hints_item = hints.update(table_override or table, columns)
            if hints_item is not None:
                yield hints_item

        ranges = partition_ranges(
            consumer, topics, offsets, start_dt, end_dt, request_timeout
        )
        for r in ranges:
            stored = offsets.get(r.topic, {}).get(str(r.partition))
            # retention or a recreated topic moved the start: store it so it is not moved again
            if stored is not None and stored != r.start - 1:
                offsets[r.topic][str(r.partition)] = r.start - 1
        for messages in read_batches(consumer, ranges, batch_size, batch_timeout):
            groups: Dict[Tuple[str, Optional[Hashable]], List[Message]] = {}
            for msg in messages:
                groups.setdefault((msg.topic(), processor.schema_id(msg)), []).append(
                    msg
                )
            for ((topic, schema_id), group), group_offset in zip(
                groups.items(), group_offsets(list(groups.values()))
            ):
                table, columns = processor.table_schema(topic, schema_id)
                table = table_override or table
                hints_item = hints.update(table, columns)
                if hints_item is not None:
                    yield hints_item
                for (offset_topic, partition), offset in group_offset.items():
                    offsets.setdefault(offset_topic, {})[str(partition)] = offset
                items = processor(table, schema_id, hints.columns(table), group)
                if table_override:
                    items = [
                        dlt.mark.with_table_name(_unmarked(item), table_override)
                        for item in items
                    ]
                yield from items


def _current_resource() -> Optional[DltResource]:
    try:
        return dlt.current.resource()
    except DltException:
        return None


def _naming() -> NamingConvention:
    try:
        return dlt.current.source_schema().naming
    except DltException:
        return SnakeCaseNamingConvention()


def _static_table_name(resource: Optional[DltResource]) -> Optional[str]:
    """Table name the user applied to the resource, all topics load into it"""
    if resource is None:
        return None
    table_name = resource.table_name
    if isinstance(table_name, str) and table_name != resource.name:
        return table_name
    return None


def _unmarked(item: TDataItem) -> TDataItem:
    return item.data if isinstance(item, DataItemWithMeta) else item
