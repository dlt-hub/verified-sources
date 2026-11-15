from datetime import datetime
from typing import Any, Dict, Hashable, List, Optional, Tuple

import dlt
import pytest
from confluent_kafka import KafkaError, KafkaException, Message
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TDataItem
from dlt.pipeline.exceptions import PipelineStepFailed

from sources.kafka import BatchProcessor, kafka_consumer

from tests.kafka.utils import (
    BASE_TS_MS,
    FakeConsumer,
    FakePartition,
    UseFakeConsumer,
    as_datetime,
    exception_chain,
    last_row_count,
    loaded_offsets,
    make_message,
    make_partition,
    raised_by,
    stored_offsets,
)
from tests.utils import assert_load_info

TOPIC = "orders"


def ts(offset: int) -> datetime:
    """Timestamp of the message at `offset` made by `make_message`"""
    return as_datetime(BASE_TS_MS + offset * 1000)


@pytest.mark.parametrize(
    "partitions,expected",
    [
        ([(1, 0)], [(0, 0)]),
        ([(1, 0), (1, 0)], [(0, 0), (1, 0)]),
        ([(0, 0), (3, 0)], [(1, 0), (1, 1), (1, 2)]),
        ([(2, 5)], [(0, 5), (0, 6)]),
    ],
    ids=[
        "single_message",
        "one_message_per_partition",
        "empty_partition",
        "log_starts_at_5",
    ],
)
def test_reads_to_the_end(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    partitions: List[Any],
    expected: List[Any],
) -> None:
    consumer = FakeConsumer(
        {
            TOPIC: [
                make_partition(TOPIC, p, count, low)
                for p, (count, low) in enumerate(partitions)
            ]
        }
    )
    assert_load_info(
        pipeline.run(kafka_consumer(TOPIC, credentials=use_fake_consumer(consumer)))
    )
    assert loaded_offsets(pipeline, TOPIC) == expected
    assert consumer.closed


def test_add_topic(pipeline: dlt.Pipeline, use_fake_consumer: UseFakeConsumer) -> None:
    topics = {
        TOPIC: [make_partition(TOPIC, 0, 3)],
        "returns": [make_partition("returns", 0, 2)],
    }
    credentials = use_fake_consumer(FakeConsumer(topics))
    pipeline.run(kafka_consumer(TOPIC, credentials=credentials))

    credentials = use_fake_consumer(FakeConsumer(topics))
    assert_load_info(
        pipeline.run(kafka_consumer([TOPIC, "returns"], credentials=credentials))
    )
    assert loaded_offsets(pipeline, "returns") == [(0, 0), (0, 1)]
    assert last_row_count(pipeline, TOPIC) == 0
    assert stored_offsets(pipeline) == {TOPIC: {"0": 2}, "returns": {"0": 1}}


def test_start_from_is_lower_bound(
    pipeline: dlt.Pipeline, use_fake_consumer: UseFakeConsumer
) -> None:
    topics = {TOPIC: [make_partition(TOPIC, 0, 10)]}
    for _ in range(2):
        credentials = use_fake_consumer(FakeConsumer(topics))
        pipeline.run(kafka_consumer(TOPIC, credentials=credentials, start_from=ts(4)))
    assert loaded_offsets(pipeline, TOPIC) == [(0, offset) for offset in range(4, 10)]

    # a later `start_from` skips messages after the stored offset
    topics[TOPIC][0].records.extend(
        make_message(TOPIC, 0, offset) for offset in range(10, 15)
    )
    credentials = use_fake_consumer(FakeConsumer(topics))
    pipeline.run(kafka_consumer(TOPIC, credentials=credentials, start_from=ts(12)))
    assert loaded_offsets(pipeline, TOPIC)[-3:] == [(0, 12), (0, 13), (0, 14)]
    assert last_row_count(pipeline, TOPIC) == 3


def test_transaction_markers_end_the_run(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    kafka_warnings: List[str],
) -> None:
    # partition 0 ends with a commit marker, partition 1 gets a message on every consume call
    consumer = FakeConsumer(
        {
            TOPIC: [
                make_partition(TOPIC, 0, 6, markers=[5]),
                make_partition(TOPIC, 1, 2),
            ]
        },
        live=[(TOPIC, 1)],
        # a consumer passed by the user, without end of partition events
        partition_eof=False,
    )
    assert_load_info(
        pipeline.run(kafka_consumer(TOPIC, credentials=use_fake_consumer(consumer)))
    )
    # without end of partition events the position past the marker ends the partition
    assert consumer.consume_calls < 20
    assert not any("read to the end" in warning for warning in kafka_warnings)
    assert [offset for p, offset in loaded_offsets(pipeline, TOPIC) if p == 0] == list(
        range(5)
    )


@pytest.mark.parametrize(
    "error,raises",
    [
        (KafkaError(KafkaError._TRANSPORT, retriable=True), False),
        (KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED), True),
        (KafkaError(KafkaError._FATAL, fatal=True), True),
    ],
    ids=["retriable", "not_retriable", "fatal"],
)
def test_message_errors(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    kafka_warnings: List[str],
    error: KafkaError,
    raises: bool,
) -> None:
    consumer = FakeConsumer({TOPIC: [make_partition(TOPIC, 0, 3)]}, errors=[error])
    resource = kafka_consumer(TOPIC, credentials=use_fake_consumer(consumer))
    if raises:
        raised = raised_by(lambda: pipeline.run(resource))
        assert isinstance(raised, PipelineStepFailed)
        assert any(isinstance(ex, KafkaException) for ex in exception_chain(raised))
    else:
        assert_load_info(pipeline.run(resource))
        assert len(loaded_offsets(pipeline, TOPIC)) == 3
        assert any(error.str() in message for message in kafka_warnings)


@pytest.mark.parametrize(
    "window,expected",
    [
        ({"start_from": ts(3), "end_time": ts(6)}, [3, 4, 5]),
        ({"end_time": ts(2)}, [0, 1]),
    ],
    ids=["start_and_end", "end_only"],
)
def test_end_time_window_is_stateless(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    window: Dict[str, datetime],
    expected: List[int],
) -> None:
    topics = {TOPIC: [make_partition(TOPIC, 0, 9)]}
    pipeline.run(
        kafka_consumer(
            TOPIC, credentials=use_fake_consumer(FakeConsumer(topics)), **window
        )
    )
    assert loaded_offsets(pipeline, TOPIC) == [(0, offset) for offset in expected]
    assert stored_offsets(pipeline) == {}

    # the next run without a window starts from the beginning
    pipeline.run(
        kafka_consumer(TOPIC, credentials=use_fake_consumer(FakeConsumer(topics)))
    )
    assert last_row_count(pipeline, TOPIC) == 9


class ValueSchemaProcessor(BatchProcessor):
    """Schema id is the first byte of the value, records the groups it gets"""

    def __init__(self) -> None:
        self.groups: List[Tuple[str, Optional[Hashable], List[int]]] = []

    def schema_id(self, msg: Message) -> Optional[Hashable]:
        return msg.value()[:1]

    def __call__(
        self,
        table: str,
        schema_id: Optional[Hashable],
        columns: Optional[TTableSchemaColumns],
        messages: List[Message],
    ) -> List[TDataItem]:
        self.groups.append((table, schema_id, [msg.offset() for msg in messages]))
        rows = [
            {"_kafka": {"partition": m.partition(), "offset": m.offset()}}
            for m in messages
        ]
        return [dlt.mark.with_table_name(rows, table)]


def test_groups_by_schema(
    pipeline: dlt.Pipeline, use_fake_consumer: UseFakeConsumer
) -> None:
    values = [b"a1", b"b2", b"a3", b"a4", b"b5"]
    partition = FakePartition(
        [
            make_message(TOPIC, 0, offset, value=value)
            for offset, value in enumerate(values)
        ]
    )
    processor = ValueSchemaProcessor()
    pipeline.run(
        kafka_consumer(
            TOPIC,
            credentials=use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
            msg_processor=processor,
        )
    )
    assert processor.groups == [(TOPIC, b"a", [0, 2, 3]), (TOPIC, b"b", [1, 4])]
    # the last consumed offset is stored although the last group ends earlier
    assert stored_offsets(pipeline) == {TOPIC: {"0": 4}}


def test_add_limit_keeps_later_groups(
    pipeline: dlt.Pipeline, use_fake_consumer: UseFakeConsumer
) -> None:
    values = [b"a0", b"b1", b"a2", b"a3", b"b4"]
    partition = FakePartition(
        [
            make_message(TOPIC, 0, offset, value=value)
            for offset, value in enumerate(values)
        ]
    )

    def run(limit: Optional[int]) -> None:
        resource = kafka_consumer(
            TOPIC,
            credentials=use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
            msg_processor=ValueSchemaProcessor(),
        )
        pipeline.run(resource.add_limit(limit) if limit else resource)

    # the limit stops the run after the first group of the batch
    run(1)
    run(None)
    # a group cut off by the limit loads on the next run, the groups before it may load again
    assert {offset for _, offset in loaded_offsets(pipeline, TOPIC)} == set(range(5))
    assert stored_offsets(pipeline) == {TOPIC: {"0": 4}}


def test_function_processor_names_tables(
    pipeline: dlt.Pipeline, use_fake_consumer: UseFakeConsumer
) -> None:
    def to_table(msg: Message) -> Dict[str, Any]:
        topic = "even" if msg.offset() % 2 == 0 else "odd"
        return {"_kafka": {"topic": topic, "offset": msg.offset()}}

    consumer = FakeConsumer({TOPIC: [make_partition(TOPIC, 0, 5)]})
    pipeline.run(
        kafka_consumer(
            TOPIC, credentials=use_fake_consumer(consumer), msg_processor=to_table
        )
    )
    assert sorted(pipeline.default_schema.data_table_names()) == ["even", "odd"]


def test_static_table_name(
    pipeline: dlt.Pipeline, use_fake_consumer: UseFakeConsumer
) -> None:
    topics = {
        name: [make_partition(name, 0, 2)] for name in ("first.topic", "second.topic")
    }
    resource = kafka_consumer(
        list(topics), credentials=use_fake_consumer(FakeConsumer(topics))
    )
    resource.apply_hints(table_name="all_topics")
    pipeline.run(resource)
    assert pipeline.default_schema.data_table_names() == ["all_topics"]
    assert len(loaded_offsets(pipeline, "all_topics")) == 4
