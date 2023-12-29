import pytest
import random
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

import dlt
from dlt.common import pendulum
from sources.kafka import kafka_consumer
from sources.kafka.helpers import (
    KafkaCredentials,
    OffsetTracker,
)
from tests.utils import assert_load_info, assert_query_data, load_table_counts

KAFKA_CONSUMER_NAME = "dlt_ci_kafka_source"


def _await(res):
    for future in res.values():
        future.result()


def _creds_to_conf(creds):
    return {
        "bootstrap.servers": creds.bootstrap_servers,
        "security.protocol": creds.security_protocol,
        "sasl.mechanisms": creds.sasl_mechanisms,
        "sasl.username": creds.sasl_username,
        "sasl.password": creds.sasl_password,
    }


def _random_name(prefix):
    name = ""
    for _ in range(9):
        name += str(random.randint(0, 9))

    return f"test_{prefix}_{name}"


@pytest.fixture(scope="module")
def kafka_admin():
    """Init an admin client - for topics creation and deletion."""
    credentials = dlt.secrets.get("sources.kafka.credentials", KafkaCredentials)
    return AdminClient(_creds_to_conf(credentials))


@pytest.fixture(scope="module")
def kafka_producer():
    credentials = dlt.secrets.get("sources.kafka.credentials", KafkaCredentials)
    return Producer(_creds_to_conf(credentials))


@pytest.fixture(scope="function")
def kafka_topics(kafka_admin):
    t_names = []
    new_topics = []

    for _ in range(2):
        name = _random_name("topic")
        new_topics.append(NewTopic(name, num_partitions=2))
        t_names.append(name)

    _await(kafka_admin.create_topics(new_topics))
    time.sleep(10)

    try:
        yield t_names
    finally:
        _await(kafka_admin.delete_topics(t_names))


@pytest.fixture(scope="function")
def kafka_messages(kafka_topics, kafka_producer):
    keys = {}

    for topic in kafka_topics:
        keys[topic] = []
        for i in range(3):
            key = str(random.randint(1, 1000))
            kafka_producer.produce(topic, b"value" + str(i).encode(), key.encode())
            keys[topic].append(key)

    kafka_producer.flush()
    return keys


@pytest.fixture(scope="function")
def kafka_timed_messages(kafka_admin, kafka_producer):
    """Produce several messages with noticeable pause between them."""
    ts = None

    topic = _random_name("topic")
    _await(kafka_admin.create_topics([NewTopic(topic, num_partitions=1)]))
    time.sleep(10)

    for i in range(3):
        key = str(i)
        kafka_producer.produce(topic, b"value" + key.encode(), key.encode())
        kafka_producer.flush()

        if i == 1:
            time.sleep(10)
            ts = pendulum.now(tz="UTC")

        time.sleep(10)

    yield topic, ts

    _await(kafka_admin.delete_topics([topic]))


def test_kafka_read(kafka_topics, kafka_messages):
    """Test simple messages reading."""
    pipeline = dlt.pipeline(
        pipeline_name="kafka_test",
        destination="postgres",
        dataset_name="kafka_test_data",
        full_refresh=True,
    )

    resource = kafka_consumer(kafka_topics)
    load_info = pipeline.run(resource)

    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert set(table_counts.keys()) == set(kafka_topics)

    for tab in table_counts:
        assert table_counts[tab] == 3

    for tab in table_names:
        assert_query_data(
            pipeline,
            f"SELECT _kafka__key FROM {tab} ORDER BY _kafka__key",
            sorted(kafka_messages[tab]),
        )


def test_kafka_read_custom_msg_processor(kafka_topics, kafka_messages):
    """
    Test messages reading and processing with a
    custom processor-function.
    """

    def _custom_msg_processor(msg):
        return {
            "_kafka": {
                "topic": msg.topic(),
            },
            "value": "test!",
        }

    pipeline = dlt.pipeline(
        pipeline_name="kafka_test",
        destination="postgres",
        dataset_name="kafka_test_data",
        full_refresh=True,
    )
    resource = kafka_consumer(kafka_topics, msg_processor=_custom_msg_processor)
    load_info = pipeline.run(resource)

    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]

    for tab in table_names:
        assert_query_data(
            pipeline,
            f"SELECT value FROM {tab}",
            ["test!", "test!", "test!"],
        )


def test_kafka_read_with_timestamp(kafka_timed_messages):
    """Test if offset is set correctly from a timestamp."""
    topic, ts = kafka_timed_messages

    credentials = dlt.secrets.get("sources.kafka.credentials", KafkaCredentials)
    consumer = credentials.init_consumer()

    tracker = OffsetTracker(consumer, [topic], {}, start_from=ts)

    assert tracker[topic]["0"] == {"cur": 2, "max": 3}


def test_kafka_incremental_read(kafka_producer, kafka_topics):
    """Test incremental messages reading.

    Every test topic has two partitions. If we produce
    messages without key, messages are distributed between
    partitions in Round Robin fashion. Thus, during the test
    we add one message into every partition.
    """
    topic1 = kafka_topics[0]
    topic2 = kafka_topics[1]

    # produce 2 messages for every topic
    for topic in kafka_topics:
        for _ in range(2):
            kafka_producer.produce(topic, b"value", None)

    kafka_producer.flush()
    _extract_assert(kafka_topics, {topic1: 2, topic2: 2})

    # produce 2 messages for the first topic
    for _ in range(2):
        kafka_producer.produce(topic1, b"value", None)

    kafka_producer.flush()
    _extract_assert(kafka_topics, {topic1: 4, topic2: 2})

    # produce 2 messages for the second topic
    for _ in range(2):
        kafka_producer.produce(topic2, b"value", None)

    kafka_producer.flush()
    _extract_assert(kafka_topics, {topic1: 4, topic2: 4})

    # produce 2 messages for every topic
    for topic in kafka_topics:
        for _ in range(2):
            kafka_producer.produce(topic, b"value", None)

    kafka_producer.flush()
    _extract_assert(kafka_topics, {topic1: 6, topic2: 6})


def _extract_assert(topics, expected):
    pipeline = dlt.pipeline(
        pipeline_name="kafka_test",
        destination="postgres",
        dataset_name="kafka_test_data",
    )

    resource = kafka_consumer(topics)
    pipeline.run(resource)

    table_counts = load_table_counts(pipeline, *topics)
    assert table_counts == expected
