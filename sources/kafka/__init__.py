"""A source to extract Kafka messages.

When extraction starts, partitions length is checked -
data is read only up to it, overriding the default Kafka's
behavior of waiting for new messages in endless loop.
"""
from typing import Callable, Dict, Iterable, List, Optional

from confluent_kafka import Consumer

import dlt
from dlt.common.typing import TDataItem
from .helpers import default_message_processor, init_consumer, OffsetTracker


@dlt.resource(name="kafka_messages", standalone=True)
def kafka_consumer(
    topics: list,
    credentials: Optional[dict] = dlt.secrets.value,
    group_id: Optional[str] = dlt.config.value,
    consumer: Optional[Consumer] = None,
    msg_processor: Optional[Callable] = default_message_processor,
) -> Iterable[TDataItem]:
    """Extract recent messages from the given Kafka topics.

    The source tracks offsets for all the topics and partitions,
    and so reads data incrementally.

    Messages from different topics are saved in different tables.

    Args:
        topics (list): Names of topics to extract.
        credentials (Optional[dict]): Credentials for a Kafka consumer. By
            default, are taken from secrets.
        group_id (Optional[str]): Consumer group id. By default, is taken
            from config.
        consumer (Optional[confluent_kafka.Consumer]): A consumer to be
            used for extraction. By default, will be initiated with the
            credentials kept in secrets.
        msg_processor(Optional[Callable]): A function-converter,
            which'll process every Kafka message after it's read and
            before it's transfered to the destination.

    Yields:
        Iterable[TDataItem]: Kafka messages.
    """
    consumer = consumer or init_consumer(credentials, group_id)
    consumer.subscribe(topics)

    tracker = OffsetTracker(consumer, topics)

    while tracker.has_unread:
        for msg in consumer.consume(num_messages=3000, timeout=1):
            if msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                message = msg_processor(msg)
                yield dlt.mark.with_table_name(message, msg.topic())

                tracker.update(msg)

    consumer.close()
