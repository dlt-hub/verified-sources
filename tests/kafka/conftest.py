from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from uuid import uuid4

import dlt
import pytest
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    ScramCredentialInfo,
    ScramMechanism,
    UserScramCredentialUpsertion,
)
from dlt.common import logger

from sources.kafka import KafkaCredentials, SchemaRegistryCredentials
from sources.kafka.helpers import hints as hints_module

from tests.kafka.utils import (
    BASE_TS_MS,
    LOCAL_KAFKA,
    LOCAL_SCHEMA_REGISTRY,
    SCRAM_USER,
    FakeConsumer,
    SeededMessage,
)


@pytest.fixture(scope="session")
def kafka_admin() -> AdminClient:
    """Admin client of the local Kafka of tests/kafka/docker-compose.yml"""
    admin = AdminClient({"bootstrap.servers": LOCAL_KAFKA})
    try:
        admin.list_topics(timeout=5)
    except KafkaException as ex:
        raise RuntimeError(
            f"Kafka is not running at {LOCAL_KAFKA}, start it with "
            "`tests/kafka/certs/generate.sh && "
            "docker compose -f tests/kafka/docker-compose.yml up -d --wait`"
        ) from ex
    return admin


@pytest.fixture(scope="session")
def local_registry(kafka_admin: AdminClient) -> Any:
    """Client of the Schema Registry of tests/kafka/docker-compose.yml"""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    client = SchemaRegistryClient({"url": LOCAL_SCHEMA_REGISTRY})
    try:
        client.get_subjects()
    except Exception as ex:
        raise RuntimeError(
            f"Schema Registry is not running at {LOCAL_SCHEMA_REGISTRY}, start it with "
            "`docker compose -f tests/kafka/docker-compose.yml up -d --wait`"
        ) from ex
    return client


@pytest.fixture
def local_registry_credentials() -> SchemaRegistryCredentials:
    credentials = SchemaRegistryCredentials(url=LOCAL_SCHEMA_REGISTRY)
    credentials.resolve()
    return credentials


@pytest.fixture
def mock_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> Tuple[Any, SchemaRegistryCredentials]:
    """In-memory registry client that `SchemaRegistryCredentials.init_client` returns"""
    from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (
        MockSchemaRegistryClient,
    )

    client = MockSchemaRegistryClient({"url": "mock://"})
    monkeypatch.setattr(SchemaRegistryCredentials, "init_client", lambda self: client)
    credentials = SchemaRegistryCredentials(url="mock://")
    credentials.resolve()
    return client, credentials


@pytest.fixture
def make_topic(kafka_admin: AdminClient) -> Iterator[Callable[..., str]]:
    """Creates topics with unique names, deletes them after the test"""
    created: List[str] = []

    def _make_topic(
        partitions: int = 1, name: Optional[str] = None, **config: str
    ) -> str:
        name = name or f"dlt_test_{uuid4().hex[:8]}"
        topic = NewTopic(name, num_partitions=partitions, config=config)
        for future in kafka_admin.create_topics([topic], operation_timeout=10).values():
            future.result()
        created.append(name)
        return name

    yield _make_topic
    if created:
        for future in kafka_admin.delete_topics(created, operation_timeout=10).values():
            try:
                future.result()
            except KafkaException:
                pass


@pytest.fixture
def producer(kafka_admin: AdminClient) -> Producer:
    # idempotence keeps the order of messages in a partition when a batch is retried
    return Producer(
        {"bootstrap.servers": LOCAL_KAFKA, "linger.ms": 5, "enable.idempotence": True}
    )


@pytest.fixture
def transactional_producer(kafka_admin: AdminClient) -> Producer:
    producer = Producer(
        {
            "bootstrap.servers": LOCAL_KAFKA,
            "transactional.id": f"dlt_test_{uuid4().hex[:8]}",
        }
    )
    producer.init_transactions(30)
    return producer


@pytest.fixture
def local_credentials() -> KafkaCredentials:
    """Resolved credentials of the local Kafka, so dlt does not inject the ones from secrets"""
    credentials = KafkaCredentials()
    credentials.bootstrap_servers = LOCAL_KAFKA
    credentials.group_id = "dlt_test"
    credentials.security_protocol = "PLAINTEXT"
    credentials.resolve()
    return credentials


@pytest.fixture
def kafka_warnings(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """Warnings the source logs; the dlt logger does not propagate to `caplog`"""
    messages: List[str] = []
    original = logger.warning

    def _warning(msg: str, *args: Any, **kwargs: Any) -> None:
        messages.append(msg)
        original(msg, *args, **kwargs)

    monkeypatch.setattr(logger, "warning", _warning)
    return messages


@pytest.fixture
def emitted_hints(monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
    """Table hints the resource emits"""
    emitted: List[Dict[str, Any]] = []
    original = hints_module.dlt.mark.with_hints

    def _with_hints(item: Any, hints: Any, create_table_variant: bool = False) -> Any:
        emitted.append(hints)
        return original(item, hints, create_table_variant=create_table_variant)

    monkeypatch.setattr(hints_module.dlt.mark, "with_hints", _with_hints)
    return emitted


@pytest.fixture
def use_fake_consumer(
    monkeypatch: pytest.MonkeyPatch, local_credentials: KafkaCredentials
) -> Callable[[FakeConsumer], KafkaCredentials]:
    """Makes `KafkaCredentials.init_consumer` return the given fake consumer"""

    def _use(consumer: FakeConsumer) -> KafkaCredentials:
        monkeypatch.setattr(KafkaCredentials, "init_consumer", lambda self: consumer)
        return local_credentials

    return _use


@pytest.fixture
def pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=f"kafka_{uuid4().hex[:8]}", destination="duckdb", dev_mode=True
    )


@pytest.fixture
def seeded_topic(
    make_topic: Callable[..., str], producer: Producer
) -> Tuple[str, List[SeededMessage]]:
    """1050 messages over 30 days in 3 partitions, timestamps unique and increasing per partition"""
    topic = make_topic(partitions=3)
    step = 30 * 86_400_000 // 1050
    next_offset = [0, 0, 0]
    seeded = []
    for i in range(1050):
        ts_ms = BASE_TS_MS + i * step + (i * 7919) % step
        partition = (i * 7 + i // 5) % 3
        producer.produce(topic, b"seeded", b"k", partition=partition, timestamp=ts_ms)
        seeded.append(SeededMessage(partition, next_offset[partition], ts_ms))
        next_offset[partition] += 1
    producer.flush()
    return topic, seeded


@pytest.fixture(scope="session")
def scram_user(kafka_admin: AdminClient) -> None:
    upsert = UserScramCredentialUpsertion(
        SCRAM_USER[0],
        ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096),
        SCRAM_USER[1].encode(),
    )
    for future in kafka_admin.alter_user_scram_credentials([upsert]).values():
        future.result()
