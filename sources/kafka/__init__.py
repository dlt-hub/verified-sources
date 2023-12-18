"""A source to extract Kafka messages.

When extraction starts, partitions length is checked -
data is read only up to it, overriding the default Kafka's
behavior of waiting for new messages in endless loop.
"""
from typing import Callable, Dict, Iterable, List, Optional, Union

from confluent_kafka import Consumer

import dlt
from dlt.common.typing import TDataItem
from .helpers import (
    default_message_processor,
    KafkaCredentials,
    OffsetTracker,
)


@dlt.resource(
    name="kafka_messages",
    table_name=lambda msg: msg["_kafka"]["topic"],
    standalone=True,
)
def kafka_consumer(
    topics: Union[str, List[str]],
    credentials: Union[KafkaCredentials, Consumer] = dlt.secrets.value,
    msg_processor: Optional[Callable] = default_message_processor,
    batch_size: Optional[int] = 3000,
    start_from_ts: Optional[int] = None,
) -> Iterable[TDataItem]:
    """Extract recent messages from the given Kafka topics.

    The source tracks offsets for all the topics and partitions,
    and so reads data incrementally.

    Messages from different topics are saved in different tables.

    Args:
        topics (Union[str, List[str]]): Names of topics to extract.
        credentials (Union[KafkaCredentials, Consumer]): Auth credentials
            or an initiated Kafka consumer.
        msg_processor(Optional[Callable]): A function-converter,
            which'll process every Kafka message after it's read and
            before it's transfered to the destination.
        batch_size (Optional[int]): Messages batch size to read at once.
        start_from_ts (Optional[int]): A timestamp, after which messages
            are read. Older messages are ignored.

    Yields:
        Iterable[TDataItem]: Kafka messages.
    """
    if not isinstance(topics, list):
        topics = [topics]

    if isinstance(credentials, Consumer):
        consumer = consumer
    elif isinstance(credentials, KafkaCredentials):
        consumer = credentials.init_consumer()
    else:
        raise TypeError(
            "Wrong credentials type provided. The credentials need to be of type: KafkaCredentials or confluent_kafka.Consumer"
        )

    consumer = consumer or init_consumer(credentials, group_id)
    consumer.subscribe(topics)

    tracker = OffsetTracker(
        consumer, topics, dlt.current.resource_state(), start_from_ts
    )

    while tracker.has_unread:
        batch = []
        for msg in consumer.consume(batch_size, timeout=1):
            if msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                batch.append(msg_processor(msg))
                tracker.update(msg)

        yield batch

    consumer.close()
