from datetime import datetime, timedelta
from typing import Dict, Iterator, List, Mapping, NamedTuple, Optional, Sequence, Tuple

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from dlt.common import logger

from .columns import EPOCH


# message errors after which the read cannot continue, librdkafka recovers from the others
FAILING_ERRORS = frozenset(
    (
        "_FATAL",
        "_AUTHENTICATION",
        "_SSL",
        "_AUTO_OFFSET_RESET",
        "_UNKNOWN_TOPIC",
        "_UNKNOWN_PARTITION",
        "SASL_AUTHENTICATION_FAILED",
        "TOPIC_AUTHORIZATION_FAILED",
        "CLUSTER_AUTHORIZATION_FAILED",
        "UNKNOWN_TOPIC_OR_PART",
    )
)

TStoredOffsets = Mapping[str, Mapping[str, int]]
"""Last read offset per topic and partition, partitions are str keys as kept in the state"""


class PartitionRange(NamedTuple):
    """Offsets of a partition to read: from `start` up to `end`, excluding `end`"""

    topic: str
    partition: int
    start: int
    end: int


def to_epoch_ms(value: datetime) -> int:
    return (value - EPOCH) // timedelta(milliseconds=1)


def offsets_for_time(
    consumer: Consumer,
    topic: str,
    partitions: Sequence[int],
    at: datetime,
    timeout: float,
) -> Dict[int, int]:
    """First offset of each partition with a timestamp at or after `at`, -1 when there is none"""
    at_ms = to_epoch_ms(at)
    offsets = consumer.offsets_for_times(
        [TopicPartition(topic, partition, at_ms) for partition in partitions],
        timeout=timeout,
    )
    result = {}
    for tp in offsets:
        if tp.error is not None:
            raise KafkaException(tp.error)
        result[tp.partition] = tp.offset
    return result


def topic_partitions(
    consumer: Consumer, topics: Sequence[str], timeout: float
) -> Dict[str, List[int]]:
    """Partitions of `topics`, raises when a topic does not exist or its metadata fails"""
    metadata = consumer.list_topics(timeout=timeout)
    result = {}
    for topic in topics:
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None:
            raise ValueError(
                f"Topic `{topic}` does not exist or the credentials are not allowed to describe it"
            )
        if topic_metadata.error is not None:
            raise KafkaException(topic_metadata.error)
        result[topic] = sorted(topic_metadata.partitions)
    return result


def watermark_offsets(
    consumer: Consumer, tp: TopicPartition, timeout: float
) -> Tuple[int, int]:
    """Low and high watermark, the high one is the last stable offset under `read_committed`"""
    watermarks = consumer.get_watermark_offsets(tp, timeout=timeout, cached=False)
    if watermarks is None:
        raise KafkaException(
            KafkaError(
                KafkaError._TIMED_OUT,
                f"Timed out reading offsets of topic `{tp.topic}` partition {tp.partition}",
            )
        )
    return watermarks


def partition_ranges(
    consumer: Consumer,
    topics: Sequence[str],
    stored_offsets: TStoredOffsets,
    start_from: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    timeout: float = 30.0,
) -> List[PartitionRange]:
    """Offsets to read from each partition of `topics`.

    Reading starts after the stored offset of a partition, or at its first message. `start_from`
    moves the start to the first message at or after it, `end_time` ends the range before the
    first message at or after it. Without `end_time` the range ends at the high watermark.

    Args:
        consumer (Consumer): Consumer used to read topic metadata and offsets.
        topics (Sequence[str]): Names of topics to read.
        stored_offsets (TStoredOffsets): Last read offset per topic and partition.
        start_from (Optional[datetime]): Start at the first message at or after this time.
        end_time (Optional[datetime]): End before the first message at or after this time.
        timeout (float): Timeout of each metadata and offset request, in seconds.

    Returns:
        List[PartitionRange]: Offsets to read, an empty range when there is nothing to read.
    """
    ranges = []
    for topic, partitions in topic_partitions(consumer, topics, timeout).items():
        starts = (
            offsets_for_time(consumer, topic, partitions, start_from, timeout)
            if start_from
            else {}
        )
        ends = (
            offsets_for_time(consumer, topic, partitions, end_time, timeout)
            if end_time
            else {}
        )
        topic_offsets = stored_offsets.get(topic, {})
        for partition in partitions:
            low, high = watermark_offsets(
                consumer, TopicPartition(topic, partition), timeout
            )
            start = low
            last_offset = topic_offsets.get(str(partition))
            if last_offset is not None:
                start = last_offset + 1
                if start < low:
                    logger.warning(
                        f"{low - start} messages in topic `{topic}` partition {partition} were"
                        f" deleted by retention before they were read, reading from offset {low}"
                    )
                    start = low
                elif start > high:
                    logger.warning(
                        f"Offset {last_offset} stored for topic `{topic}` partition {partition} is"
                        f" past the end of the partition at {high}, the topic was probably"
                        f" recreated, reading from offset {low}"
                    )
                    start = low
            if start_from is not None:
                start = max(
                    start, high if starts[partition] == -1 else starts[partition]
                )
            end = high if ends.get(partition, -1) == -1 else ends[partition]
            ranges.append(PartitionRange(topic, partition, start, end))
    return ranges


def group_offsets(
    groups: Sequence[List[Message]],
) -> List[Dict[Tuple[str, int], int]]:
    """Offsets to store before each group of a batch is yielded.

    A partition moves to just below its first message in a later group, so a run stopped between
    groups, for example by `add_limit`, reads the later groups again instead of skipping them.
    """
    firsts: List[Dict[Tuple[str, int], int]] = []
    lasts: List[Dict[Tuple[str, int], int]] = []
    for group in groups:
        first: Dict[Tuple[str, int], int] = {}
        last: Dict[Tuple[str, int], int] = {}
        for msg in group:
            tp = (msg.topic(), msg.partition())
            first.setdefault(tp, msg.offset())
            last[tp] = msg.offset()
        firsts.append(first)
        lasts.append(last)
    offsets: List[Dict[Tuple[str, int], int]] = []
    for index, last in enumerate(lasts):
        stored: Dict[Tuple[str, int], int] = {}
        for tp in last:
            later = [first[tp] for first in firsts[index + 1 :] if tp in first]
            if later:
                stored[tp] = min(later) - 1
            else:
                stored[tp] = max(done[tp] for done in lasts[: index + 1] if tp in done)
        offsets.append(stored)
    return offsets


def raise_on_error(error: KafkaError) -> None:
    """Raises errors the read cannot continue after, logs the others"""
    if error.fatal() or error.name() in FAILING_ERRORS:
        raise KafkaException(error)
    logger.warning(f"Kafka consumer error, the consumer will retry: {error.str()}")


def read_batches(
    consumer: Consumer,
    ranges: Sequence[PartitionRange],
    batch_size: int = 3000,
    batch_timeout: float = 3.0,
) -> Iterator[List[Message]]:
    """Reads the messages of `ranges` in batches of at most `batch_size`.

    A partition is done when the last message of its range arrives, or when the consumer position
    reaches the end of the range, which also covers transaction markers and compacted messages at
    the end. Done partitions are paused. Reading stops early when no message arrives within
    `batch_timeout`; the partitions not done yet are logged.

    Args:
        consumer (Consumer): Consumer, ideally with `enable.partition.eof` set.
        ranges (Sequence[PartitionRange]): Offsets to read.
        batch_size (int): Maximum number of messages to read at once.
        batch_timeout (float): Maximum time to wait for a batch, in seconds.

    Yields:
        List[Message]: Messages of the ranges, without errors and partition end events.
    """
    pending: Dict[Tuple[str, int], int] = {
        (r.topic, r.partition): r.end for r in ranges if r.start < r.end
    }
    if not pending:
        return
    consumer.assign(
        [
            TopicPartition(r.topic, r.partition, r.start)
            for r in ranges
            if r.start < r.end
        ]
    )

    def finish(key: Tuple[str, int]) -> None:
        del pending[key]
        consumer.pause([TopicPartition(*key)])

    while pending:
        messages = consumer.consume(batch_size, timeout=batch_timeout)
        if not messages:
            positions = consumer.position([TopicPartition(*key) for key in pending])
            for tp in positions:
                if tp.offset >= pending[(tp.topic, tp.partition)]:
                    finish((tp.topic, tp.partition))
            if pending:
                logger.warning(
                    f"No messages arrived in {batch_timeout} seconds, these partitions were not"
                    f" read to the end: {', '.join(f'{t}[{p}]' for t, p in pending)}. The next"
                    " run continues after the last message read."
                )
                break
            continue

        batch = []
        for msg in messages:
            error = msg.error()
            key = (msg.topic(), msg.partition())
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    if key in pending and msg.offset() >= pending[key]:
                        finish(key)
                else:
                    raise_on_error(error)
                continue
            end = pending.get(key)
            # messages of done partitions can still arrive from the fetch queue
            if end is None:
                continue
            if msg.offset() >= end:
                finish(key)
                continue
            batch.append(msg)
            if msg.offset() + 1 >= end:
                finish(key)
        if batch:
            yield batch
