from typing import Any, Dict, List

import dlt
import pytest
from confluent_kafka import KafkaError, KafkaException
from dlt.pipeline.exceptions import PipelineStepFailed

from sources.kafka import JsonProcessor, kafka_consumer

from tests.kafka.utils import (
    FakeConsumer,
    MockRegistry,
    UseFakeConsumer,
    exception_chain,
    last_row_count,
    loaded_offsets,
    make_partition,
    raised_by,
    record_partition,
    register,
)
from tests.utils import assert_load_info

TOPIC = "orders"


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


def test_add_limit_loads_every_message_once(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    emitted_hints: List[Dict[str, Any]],
) -> None:
    registry, registry_credentials = mock_registry
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    schema_id = register(registry, f"{TOPIC}-value", schema).schema_id
    partition = record_partition(
        TOPIC, [{"id": i} for i in range(5)], schema_ids=[schema_id] * 5
    )
    row_counts = []
    for _ in range(4):
        resource = kafka_consumer(
            TOPIC,
            credentials=use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
            msg_processor=JsonProcessor(),
            schema_registry=registry_credentials,
            batch_size=2,
        )
        pipeline.run(resource.add_limit(1))
        row_counts.append(last_row_count(pipeline, TOPIC))
    # the hints ride on the first batch of each run, which `add_limit` counts as the one item
    assert row_counts == [2, 2, 1, 0]
    assert len(emitted_hints) == 3
    assert loaded_offsets(pipeline, TOPIC) == [(0, offset) for offset in range(5)]
