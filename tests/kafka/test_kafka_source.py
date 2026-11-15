import json
import threading
import time
from datetime import date, datetime, timezone
from datetime import time as dt_time
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import dlt
import pytest
from confluent_kafka import (
    OFFSET_INVALID,
    Consumer,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic
from dlt.common import Decimal
from dlt.common.schema.exceptions import DataValidationError
from dlt.pipeline.exceptions import PipelineStepFailed

from sources.kafka import (
    AvroProcessor,
    IncompatibleColumnTypeException,
    JsonProcessor,
    KafkaCredentials,
    SchemaRegistryCredentials,
    kafka_consumer,
)
from sources.kafka.helpers.processors import TBackend

from tests.kafka.utils import (
    BACKENDS,
    BASE_TS_MS,
    LOCAL_KAFKA,
    MakeTopic,
    SeededMessage,
    as_datetime,
    auth_credentials,
    avro_framed,
    column_types,
    exception_chain,
    last_row_count,
    loaded_offsets,
    produce,
    produce_registry,
    run_in_thread,
    run_processor,
    stored_offsets,
    table_rows,
)
from tests.utils import assert_load_info, skip_if_not_active, skipifwindows

# CI starts the local Kafka of tests/kafka/docker-compose.yml only in Linux jobs with duckdb
pytestmark = [skipifwindows, skip_if_not_active("duckdb")]


@pytest.fixture
def live_producer(make_topic: MakeTopic) -> Iterator[Callable[[str, int], None]]:
    """Produces to a partition every 5 ms until the test ends"""
    stop = threading.Event()
    threads: List[threading.Thread] = []

    def _start(topic: str, partition: int) -> None:
        def _produce() -> None:
            producer = Producer({"bootstrap.servers": LOCAL_KAFKA, "linger.ms": 5})
            while not stop.is_set():
                try:
                    producer.produce(topic, b"live", b"k", partition=partition)
                except BufferError:
                    pass
                producer.poll(0)
                time.sleep(0.005)
            producer.flush()

        thread = threading.Thread(target=_produce, daemon=True)
        thread.start()
        threads.append(thread)

    yield _start
    stop.set()
    for thread in threads:
        thread.join(10)


@pytest.mark.parametrize(
    "counts,stored",
    [([1], {"0": 1}), ([1, 1], {"0": 0, "1": 1}), ([0, 3], {"1": 3})],
    ids=["single_message", "one_message_per_partition", "empty_partition"],
)
def test_local_reads_to_the_end_and_next_runs(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    counts: List[int],
    stored: Dict[str, int],
) -> None:
    topic = make_topic(partitions=len(counts))
    for partition, count in enumerate(counts):
        produce(producer, topic, count, partition=partition)
    assert_load_info(pipeline.run(kafka_consumer(topic, credentials=local_credentials)))
    assert loaded_offsets(pipeline, topic) == [
        (partition, offset)
        for partition, count in enumerate(counts)
        for offset in range(count)
    ]

    produce(producer, topic, 1, partition=len(counts) - 1)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert last_row_count(pipeline, topic) == 1

    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert last_row_count(pipeline, topic) == 0
    # an empty partition has no stored offset
    assert stored_offsets(pipeline) == {topic: stored}


def test_local_add_topic(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    topic, added = make_topic(), make_topic()
    produce(producer, topic, 3)
    produce(producer, added, 2)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))

    assert_load_info(
        pipeline.run(kafka_consumer([topic, added], credentials=local_credentials))
    )
    assert loaded_offsets(pipeline, added) == [(0, 0), (0, 1)]
    assert last_row_count(pipeline, topic) == 0
    assert stored_offsets(pipeline) == {topic: {"0": 2}, added: {"0": 1}}


@pytest.mark.parametrize(
    "after", [1, 3], ids=["one_message_after", "three_messages_after"]
)
def test_local_start_from(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    after: int,
) -> None:
    # partition 0 ends before `start_from`, partition 1 has `after` messages after it
    topic = make_topic(partitions=2)
    start_ms = BASE_TS_MS + 100_000
    produce(producer, topic, 3, partition=0, timestamp_ms=lambda i: BASE_TS_MS + i)
    produce(producer, topic, 1, partition=1, timestamp_ms=lambda i: BASE_TS_MS)
    produce(producer, topic, after, partition=1, timestamp_ms=lambda i: start_ms + i)

    for _ in range(2):
        pipeline.run(
            kafka_consumer(
                topic, credentials=local_credentials, start_from=as_datetime(start_ms)
            )
        )
        assert loaded_offsets(pipeline, topic) == [
            (1, offset) for offset in range(1, after + 1)
        ]

    # a later `start_from` skips messages after the stored offset
    later_ms = start_ms + 100_000
    produce(producer, topic, 2, partition=1, timestamp_ms=lambda i: later_ms - 1)
    produce(producer, topic, 3, partition=1, timestamp_ms=lambda i: later_ms + i)
    pipeline.run(
        kafka_consumer(
            topic, credentials=local_credentials, start_from=as_datetime(later_ms)
        )
    )
    assert last_row_count(pipeline, topic) == 3
    assert loaded_offsets(pipeline, topic)[-3:] == [
        (1, offset) for offset in range(after + 3, after + 6)
    ]


@pytest.mark.parametrize(
    "partition_eof", [True, False], ids=["credentials", "consumer_without_eof"]
)
def test_local_transaction_marker_with_live_partition(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    transactional_producer: Producer,
    live_producer: Callable[[str, int], None],
    local_credentials: KafkaCredentials,
    kafka_warnings: List[str],
    partition_eof: bool,
) -> None:
    topic = make_topic(partitions=2)
    transactional_producer.begin_transaction()
    produce(transactional_producer, topic, 5, partition=0)
    transactional_producer.commit_transaction(30)
    live_producer(topic, 1)
    time.sleep(1)

    credentials: Union[KafkaCredentials, Consumer] = local_credentials
    if not partition_eof:
        # a consumer passed by the user: the position past the marker ends partition 0
        local_credentials.consumer_config = {"enable.partition.eof": False}
        credentials = local_credentials.init_consumer()
    finished, result = run_in_thread(
        lambda: pipeline.run(kafka_consumer(topic, credentials=credentials)), 30
    )
    assert finished, "the run did not end while partition 1 receives messages"
    assert "error" not in result
    assert not any("read to the end" in warning for warning in kafka_warnings)
    assert [offset for p, offset in loaded_offsets(pipeline, topic) if p == 0] == list(
        range(5)
    )
    if isinstance(credentials, Consumer):
        with pytest.raises(RuntimeError, match="Consumer closed"):
            credentials.assignment()


def test_local_open_transaction(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    transactional_producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    topic = make_topic()
    produce(producer, topic, 2)
    transactional_producer.begin_transaction()
    produce(transactional_producer, topic, 3)
    try:
        finished, result = run_in_thread(
            lambda: pipeline.run(kafka_consumer(topic, credentials=local_credentials)),
            30,
        )
        assert finished and "error" not in result
        # read_committed reads up to the last stable offset, before the open transaction
        assert loaded_offsets(pipeline, topic) == [(0, 0), (0, 1)]
    finally:
        transactional_producer.abort_transaction(30)


@pytest.mark.parametrize("failure", ["unreachable_broker", "wrong_password"])
def test_local_broker_failure(
    pipeline: dlt.Pipeline,
    local_credentials: KafkaCredentials,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    monkeypatch.setenv("SOURCES__KAFKA__REQUEST_TIMEOUT", "5")
    if failure == "unreachable_broker":
        credentials = local_credentials
        credentials.bootstrap_servers = "localhost:1"
    else:
        credentials = auth_credentials("plain")
        credentials.sasl_password = "wrong"
    finished, result = run_in_thread(
        lambda: pipeline.run(kafka_consumer("any", credentials=credentials)), 60
    )
    assert finished, "the run hangs when it cannot connect"
    assert isinstance(result.get("error"), PipelineStepFailed)


@pytest.mark.parametrize("auth", ["plain", "scram", "mtls_pem", "mtls_files"])
def test_local_auth(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    scram_user: None,
    auth: str,
) -> None:
    topic = make_topic()
    produce(producer, topic, 3)
    assert_load_info(
        pipeline.run(kafka_consumer(topic, credentials=auth_credentials(auth)))
    )
    assert loaded_offsets(pipeline, topic) == [(0, 0), (0, 1), (0, 2)]


def test_local_missing_topic(
    pipeline: dlt.Pipeline, make_topic: MakeTopic, local_credentials: KafkaCredentials
) -> None:
    topic = make_topic()
    with pytest.raises(PipelineStepFailed) as py_ex:
        pipeline.run(
            kafka_consumer([topic, "dlt_test_missing"], credentials=local_credentials)
        )
    chain = exception_chain(py_ex.value)
    assert not any(isinstance(ex, KeyError) for ex in chain)
    assert "dlt_test_missing" in str(chain[-1])


def test_local_no_group_commit(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    topic = make_topic()
    produce(producer, topic, 5)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert last_row_count(pipeline, topic) == 5

    consumer = Consumer(
        {"bootstrap.servers": LOCAL_KAFKA, "group.id": local_credentials.group_id}
    )
    try:
        committed = consumer.committed([TopicPartition(topic, 0)], timeout=10)
    finally:
        consumer.close()
    assert committed[0].offset == OFFSET_INVALID


def test_local_offsets_deleted_by_retention(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    kafka_admin: AdminClient,
    local_credentials: KafkaCredentials,
    kafka_warnings: List[str],
) -> None:
    topic = make_topic()
    produce(producer, topic, 3)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    produce(producer, topic, 5)
    for future in kafka_admin.delete_records([TopicPartition(topic, 0, 6)]).values():
        future.result()

    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert loaded_offsets(pipeline, topic) == [(0, 0), (0, 1), (0, 2), (0, 6), (0, 7)]
    assert any(
        "3 messages" in message and topic in message for message in kafka_warnings
    )

    # a first run starts at the moved start of the log without a warning
    kafka_warnings.clear()
    first_run = dlt.pipeline(
        pipeline_name=f"{pipeline.pipeline_name}_first",
        destination="duckdb",
        dev_mode=True,
    )
    first_run.run(kafka_consumer(topic, credentials=local_credentials))
    assert loaded_offsets(first_run, topic) == [(0, 6), (0, 7)]
    assert kafka_warnings == []

    # every message is deleted, the moved start is stored and the next run does not warn
    for future in kafka_admin.delete_records([TopicPartition(topic, 0, 8)]).values():
        future.result()
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    kafka_warnings.clear()
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert kafka_warnings == []


def test_local_recreated_topic(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    kafka_admin: AdminClient,
    local_credentials: KafkaCredentials,
    kafka_warnings: List[str],
) -> None:
    topic = make_topic()
    produce(producer, topic, 6)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))

    for future in kafka_admin.delete_topics([topic], operation_timeout=10).values():
        future.result()
    deadline = time.monotonic() + 30
    while True:
        try:
            for future in kafka_admin.create_topics([NewTopic(topic, 1)]).values():
                future.result()
            break
        except KafkaException:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)
    # the empty topic moves the stored offset back to its start
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert any(
        "recreated" in message and topic in message for message in kafka_warnings
    )
    # more new messages than the old offset, none of them is skipped
    produce(Producer({"bootstrap.servers": LOCAL_KAFKA}), topic, 10)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert last_row_count(pipeline, topic) == 10


def test_local_null_and_empty(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    topic = make_topic()
    # an empty key or value is not null, a tombstone has a null value
    for key, value in [(None, b"v"), (b"k", None), (b"", b"v"), (b"k", b"")]:
        producer.produce(topic, value, key)
    producer.flush()
    assert_load_info(pipeline.run(kafka_consumer(topic, credentials=local_credentials)))
    assert table_rows(pipeline, topic, ["_kafka__key", "_kafka__data"]) == [
        (None, "v"),
        ("k", None),
        ("", "v"),
        ("k", ""),
    ]


def test_local_function_processor_names_tables(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    def to_table(msg: Message) -> Dict[str, Any]:
        table = "even" if msg.offset() % 2 == 0 else "odd"
        return {"_kafka": {"topic": table, "offset": msg.offset()}}

    topic = make_topic()
    produce(producer, topic, 5)
    pipeline.run(
        kafka_consumer(topic, credentials=local_credentials, msg_processor=to_table)
    )
    assert sorted(pipeline.default_schema.data_table_names()) == ["even", "odd"]


def test_local_static_table_name(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    topics = [make_topic(), make_topic()]
    for topic in topics:
        produce(producer, topic, 2)
    resource = kafka_consumer(topics, credentials=local_credentials)
    resource.apply_hints(table_name="all_topics")
    pipeline.run(resource)
    assert pipeline.default_schema.data_table_names() == ["all_topics"]
    assert len(loaded_offsets(pipeline, "all_topics")) == 4


def test_local_seeded_full_and_incremental(
    pipeline: dlt.Pipeline,
    seeded_topic: Tuple[str, List[SeededMessage]],
    producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    topic, seeded = seeded_topic
    expected = sorted((m.partition, m.offset) for m in seeded)
    assert_load_info(pipeline.run(kafka_consumer(topic, credentials=local_credentials)))
    assert loaded_offsets(pipeline, topic) == expected

    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert last_row_count(pipeline, topic) == 0

    # 2, 2 and 1 new messages in partitions 0, 1 and 2
    for partition, count in ((0, 2), (1, 2), (2, 1)):
        produce(producer, topic, count, partition=partition)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert last_row_count(pipeline, topic) == 5

    day_10, day_20, day_30 = (BASE_TS_MS + day * 86_400_000 for day in (10, 20, 30))
    backfill = dlt.pipeline(
        pipeline_name=f"{pipeline.pipeline_name}_backfill",
        destination="duckdb",
        dev_mode=True,
    )
    # the first window has no start
    for start, end in ((None, day_10), (day_10, day_20), (day_20, day_30)):
        window = sorted(
            (m.partition, m.offset)
            for m in seeded
            if (start is None or start <= m.ts_ms) and m.ts_ms < end
        )
        assert window
        backfill.run(
            kafka_consumer(
                topic,
                credentials=local_credentials,
                start_from=None if start is None else as_datetime(start),
                end_time=as_datetime(end),
            )
        )
        assert last_row_count(backfill, topic) == len(window)
        assert stored_offsets(backfill) == {}
    assert loaded_offsets(backfill, topic) == expected

    # windows do not store offsets, a run without a window reads every message
    backfill.run(kafka_consumer(topic, credentials=local_credentials))
    assert last_row_count(backfill, topic) == len(seeded) + 5


ROW_AVRO = {
    "type": "record",
    "name": "Row",
    "fields": [{"name": "v", "type": "string"}],
}


@pytest.mark.parametrize(
    "value_format,backend",
    [
        ("text", None),
        ("json", "python"),
        ("json", "pyarrow"),
        ("avro", "python"),
        ("avro", "pyarrow"),
    ],
    ids=["text", "json-python", "json-pyarrow", "avro-python", "avro-pyarrow"],
)
def test_local_incremental_changes(
    request: pytest.FixtureRequest,
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    local_registry_credentials: SchemaRegistryCredentials,
    value_format: str,
    backend: Any,
) -> None:
    topic = make_topic(partitions=3)
    registry = (
        request.getfixturevalue("local_registry") if value_format == "avro" else None
    )

    def produce_values(changes: Sequence[Tuple[bytes, Optional[str]]]) -> None:
        if value_format == "avro":
            values = [None if v is None else {"v": v} for _, v in changes]
            keys = [key for key, _ in changes]
            produce_registry(
                producer, registry, topic, ROW_AVRO, values, "avro", keys=keys
            )
            return
        for key, v in changes:
            if v is None or value_format == "text":
                producer.produce(topic, v and v.encode(), key)
            else:
                producer.produce(topic, json.dumps({"v": v}).encode(), key)
        producer.flush()

    def run() -> Any:
        if value_format == "text":
            return pipeline.run(kafka_consumer(topic, credentials=local_credentials))
        processor_class = AvroProcessor if value_format == "avro" else JsonProcessor
        registry_credentials = local_registry_credentials if registry else None
        processor = processor_class(backend=backend)
        return run_processor(
            pipeline, topic, local_credentials, processor, registry_credentials
        )

    produce_values([(f"key_{i}".encode(), f"v{i}") for i in range(30)])
    assert_load_info(run())
    assert last_row_count(pipeline, topic) == 30

    # 3 new keys, 2 updates of existing keys and a tombstone that deletes key_0
    produce_values(
        [
            (b"key_30", "v30"),
            (b"key_31", "v31"),
            (b"key_32", "v32"),
            (b"key_1", "v1_updated"),
            (b"key_2", "v2_updated"),
            (b"key_0", None),
        ]
    )
    run()
    assert last_row_count(pipeline, topic) == 6

    column = "_kafka__data" if value_format == "text" else "v"
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            f"SELECT _kafka__key, {column} FROM {topic}"
            " WHERE _kafka__key IN ('key_0', 'key_1', 'key_2')"
        )
    # the table is append only, earlier rows of updated and deleted keys stay
    assert sorted(rows, key=lambda r: (r[0], r[1] or "")) == [
        ("key_0", None),
        ("key_0", "v0"),
        ("key_1", "v1"),
        ("key_1", "v1_updated"),
        ("key_2", "v2"),
        ("key_2", "v2_updated"),
    ]

    run()
    assert last_row_count(pipeline, topic) == 0
    assert len(loaded_offsets(pipeline, topic)) == 36


def test_local_seeded_chunks(
    pipeline: dlt.Pipeline,
    seeded_topic: Tuple[str, List[SeededMessage]],
    local_credentials: KafkaCredentials,
) -> None:
    topic, seeded = seeded_topic
    runs = 0
    while True:
        pipeline.run(
            kafka_consumer(
                topic, credentials=local_credentials, batch_size=200
            ).add_limit(1)
        )
        count = last_row_count(pipeline, topic)
        assert count <= 200
        if count == 0:
            break
        runs += 1
        assert runs <= 50
    loaded = loaded_offsets(pipeline, topic)
    assert loaded == sorted((m.partition, m.offset) for m in seeded)
    assert runs >= len(seeded) // 200


ADDRESS_AVRO = {
    "type": "record",
    "name": "Address",
    "namespace": "com.shop",
    "fields": [{"name": "city", "type": "string"}, {"name": "zip", "type": "int"}],
}


class RegistryTypes(NamedTuple):
    processor: Callable[..., Any]
    schema: Any
    values: List[Any]
    types: Dict[str, str]
    rows: List[Tuple[Any, ...]]
    """`types` columns, key and offset of `values`"""


REGISTRY_TYPES = {
    "json": RegistryTypes(
        processor=partial(JsonProcessor, strict=True),
        schema={
            "title": "Order",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "amount": {"type": "number"},
                "created": {"type": "string", "format": "date-time"},
                "day": {"type": "string", "format": "date"},
                "note": {"type": ["string", "null"]},
                "customer": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "vip": {"type": "boolean"},
                    },
                },
                "tags": {"type": "array", "items": {"type": "string"}},
            },
        },
        values=[
            {
                "id": 1,
                "amount": 1.5,
                "created": "2026-01-01T10:00:00+02:00",
                "day": "2026-01-01",
                "note": "first",
                "customer": {"name": "ann", "vip": True},
                "tags": ["a", "b"],
            },
            {
                "id": 2,
                "amount": 2,
                "created": "2026-01-02T00:00:00Z",
                "day": "2026-01-02",
                "note": None,
                "customer": {"name": "bob", "vip": False},
                "tags": [],
            },
        ],
        # lists are `json` from the schema, no nested tables
        types={
            "id": "bigint",
            "amount": "double",
            "created": "timestamp",
            "day": "date",
            "note": "text",
            "customer__name": "text",
            "customer__vip": "bool",
            "tags": "json",
        },
        rows=[
            (
                1,
                1.5,
                datetime(2026, 1, 1, 8, tzinfo=timezone.utc),
                date(2026, 1, 1),
                "first",
                "ann",
                True,
                '["a","b"]',
                "key_0",
                0,
            ),
            (
                2,
                2.0,
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                date(2026, 1, 2),
                None,
                "bob",
                False,
                "[]",
                "key_1",
                1,
            ),
        ],
    ),
    "avro": RegistryTypes(
        processor=AvroProcessor,
        schema={
            "type": "record",
            "name": "Order",
            "namespace": "com.shop",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "orderId", "type": {"type": "string", "logicalType": "uuid"}},
                {
                    "name": "amount",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 12,
                        "scale": 2,
                    },
                },
                {
                    "name": "created",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {
                    "name": "local",
                    "type": {"type": "long", "logicalType": "local-timestamp-micros"},
                },
                {
                    "name": "nanos",
                    "type": {"type": "long", "logicalType": "timestamp-nanos"},
                },
                {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                {"name": "at", "type": {"type": "int", "logicalType": "time-millis"}},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["NEW", "PAID"],
                    },
                },
                {"name": "hash", "type": {"type": "fixed", "name": "Hash", "size": 2}},
                {"name": "note", "type": ["null", "string"]},
                {"name": "billing", "type": ["null", ADDRESS_AVRO]},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "attributes", "type": {"type": "map", "values": "long"}},
                {"name": "score", "type": ["null", "long", "string"]},
                {
                    "name": "updated",
                    "type": {
                        "type": "long",
                        "connect.name": "io.debezium.time.MicroTimestamp",
                    },
                },
                {
                    "name": "price",
                    "type": {
                        "type": "record",
                        "name": "VariableScaleDecimal",
                        "namespace": "io.debezium.data",
                        "fields": [
                            {"name": "scale", "type": "int"},
                            {"name": "value", "type": "bytes"},
                        ],
                    },
                },
            ],
        },
        values=[
            {
                "id": 1,
                "orderId": "00000000-0000-0000-0000-000000000001",
                "amount": Decimal("12.34"),
                "created": datetime(2026, 1, 1, 8, tzinfo=timezone.utc),
                "local": datetime(2026, 1, 1, 9, 30),
                "nanos": 1767225600123456789,
                "day": date(2026, 1, 1),
                "at": dt_time(10, 11, 12),
                "status": "NEW",
                "hash": b"\x00\xff",
                "note": "first",
                "billing": {"city": "Berlin", "zip": 10115},
                "tags": ["a", "b"],
                "attributes": {"weight": 3},
                "score": 5,
                "updated": 1767225600000001,
                "price": {"scale": 2, "value": b"\x04\xd2"},
            },
            {
                "id": 2,
                "orderId": "00000000-0000-0000-0000-000000000002",
                "amount": Decimal("-0.50"),
                "created": datetime(2026, 1, 2, tzinfo=timezone.utc),
                "local": datetime(2026, 1, 2),
                "nanos": 0,
                "day": date(2026, 1, 2),
                "at": dt_time(0),
                "status": "PAID",
                "hash": b"\x01\x02",
                "note": None,
                "billing": None,
                "tags": [],
                "attributes": {},
                "score": "high",
                "updated": 0,
                "price": {"scale": 0, "value": b"\x01"},
            },
        ],
        types={
            "id": "bigint",
            "order_id": "text",
            "amount": "decimal",
            "created": "timestamp",
            "local": "timestamp",
            "nanos": "timestamp",
            "day": "date",
            "at": "time",
            "status": "text",
            "hash": "binary",
            "note": "text",
            "billing__city": "text",
            "billing__zip": "bigint",
            "tags": "json",
            "attributes": "json",
            "score": "json",
            "updated": "timestamp",
            "price": "text",
        },
        rows=[
            (
                1,
                "00000000-0000-0000-0000-000000000001",
                Decimal("12.34"),
                datetime(2026, 1, 1, 8, tzinfo=timezone.utc),
                # local timestamps have no time zone
                datetime(2026, 1, 1, 9, 30),
                # nanoseconds load as microseconds
                datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc),
                date(2026, 1, 1),
                dt_time(10, 11, 12),
                "NEW",
                b"\x00\xff",
                "first",
                "Berlin",
                10115,
                '["a","b"]',
                '{"weight":3}',
                "5",
                datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc),
                "12.34",
                "key_0",
                0,
            ),
            (
                2,
                "00000000-0000-0000-0000-000000000002",
                Decimal("-0.50"),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 2),
                datetime(1970, 1, 1, tzinfo=timezone.utc),
                date(2026, 1, 2),
                dt_time(0),
                "PAID",
                b"\x01\x02",
                None,
                None,
                None,
                "[]",
                "{}",
                '"high"',
                datetime(1970, 1, 1, tzinfo=timezone.utc),
                "1",
                "key_1",
                1,
            ),
        ],
    ),
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("header", [False, True], ids=["prefix", "header"])
@pytest.mark.parametrize("value_format", ["json", "avro"])
def test_local_registry_types(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    local_registry: Any,
    local_registry_credentials: SchemaRegistryCredentials,
    value_format: str,
    header: bool,
    backend: TBackend,
) -> None:
    expected = REGISTRY_TYPES[value_format]
    topic = make_topic()
    # a tombstone has no schema and loads the metadata only
    produce_registry(
        producer,
        local_registry,
        topic,
        expected.schema,
        expected.values + [None],
        value_format,
        header=header,
    )
    assert_load_info(
        run_processor(
            pipeline,
            topic,
            local_credentials,
            expected.processor(backend=backend),
            local_registry_credentials,
        )
    )
    assert pipeline.default_schema.data_table_names() == [topic]
    types = column_types(pipeline, topic)
    assert {name: types[name] for name in expected.types} == expected.types
    rows = table_rows(
        pipeline, topic, [*expected.types, "_kafka__key", "_kafka__offset"]
    )
    assert rows == expected.rows + [(None,) * len(expected.types) + ("key_2", 2)]


EVENT_SCHEMAS = {
    # the registry accepts optional fields added to a closed JSON content model only
    "json": (
        {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "unset": {"type": "string"}},
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "unset": {"type": "string"},
                "amount": {"type": "number"},
            },
            "additionalProperties": False,
        },
    ),
    "avro": (
        {
            "type": "record",
            "name": "Event",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "unset", "type": ["null", "string"], "default": None},
            ],
        },
        {
            "type": "record",
            "name": "Event",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "unset", "type": ["null", "string"], "default": None},
                {"name": "amount", "type": ["null", "double"], "default": None},
            ],
        },
    ),
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "other_subject,expected_hints",
    [
        (False, [["amount", "id", "unset"]]),
        (True, [["id", "unset"], ["amount", "id", "unset"]]),
    ],
    ids=["topic_subject", "other_subject"],
)
@pytest.mark.parametrize("value_format", ["json", "avro"])
def test_local_registry_schema_evolution(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    local_registry: Any,
    local_registry_credentials: SchemaRegistryCredentials,
    emitted_hints: List[Dict[str, Any]],
    value_format: str,
    other_subject: bool,
    expected_hints: List[List[str]],
    backend: TBackend,
) -> None:
    topic = make_topic()
    v1, v2 = EVENT_SCHEMAS[value_format]
    produce_events = partial(
        produce_registry,
        producer,
        local_registry,
        topic,
        serializer=value_format,
        subject=f"{topic}-other" if other_subject else None,
    )
    processor_class = JsonProcessor if value_format == "json" else AvroProcessor

    def run() -> List[List[str]]:
        emitted_hints.clear()
        resource = kafka_consumer(
            topic,
            credentials=local_credentials,
            msg_processor=processor_class(backend=backend),
            schema_registry=local_registry_credentials,
        )
        resource.apply_hints(columns={"id": {"data_type": "text"}})
        pipeline.run(resource)
        return [sorted(hints["columns"]) for hints in emitted_hints]

    produce_events(v1, [{"id": 1}])
    assert run() == [["id", "unset"]]

    # writers of both versions interleave
    produce_events(v1, [{"id": 2}])
    produce_events(v2, [{"id": 3, "amount": 1.5}])
    produce_events(v1, [{"id": 4}])
    # the latest schema of `<topic>-value` before the first message, else each schema that adds
    # columns
    assert run() == expected_hints
    types = column_types(pipeline, topic)
    # columns of the schema without values, columns the user applied win
    assert (types["unset"], types["id"], types["amount"]) == ("text", "text", "double")
    assert table_rows(pipeline, topic, ["id", "amount"]) == [
        ("1", None),
        ("2", None),
        ("3", 1.5),
        ("4", None),
    ]


@pytest.mark.parametrize(
    "dlt_columns", [False, True], ids=["default", "add_dlt_columns"]
)
def test_local_backend_switch(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    monkeypatch: pytest.MonkeyPatch,
    dlt_columns: bool,
) -> None:
    if dlt_columns:
        monkeypatch.setenv("NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID", "true")
        monkeypatch.setenv("NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID", "true")
    topic = make_topic()
    produce(producer, topic, 1, value=lambda i: b'{"id": 0}')
    run_processor(
        pipeline, topic, local_credentials, JsonProcessor(backend="python"), None
    )
    produce(producer, topic, 1, value=lambda i: b'{"id": 1}')
    if not dlt_columns:
        # rows get NOT NULL `_dlt_load_id` and `_dlt_id`, Arrow tables don't
        with pytest.raises(PipelineStepFailed):
            run_processor(
                pipeline,
                topic,
                local_credentials,
                JsonProcessor(backend="pyarrow"),
                None,
            )
        return
    run_processor(
        pipeline, topic, local_credentials, JsonProcessor(backend="pyarrow"), None
    )
    assert table_rows(pipeline, topic, ["id"]) == [(0,), (1,)]


@pytest.mark.parametrize("isolation_level", ["read_committed", "read_uncommitted"])
def test_local_aborted_transactions(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    transactional_producer: Producer,
    local_credentials: KafkaCredentials,
    isolation_level: str,
) -> None:
    topic = make_topic()
    # markers take offsets too; the coordinator writes them after the transaction ends, so
    # the plain messages may come before the first abort marker
    transactional_producer.begin_transaction()
    produce(transactional_producer, topic, 3, value=lambda i: f"aborted_{i}".encode())
    transactional_producer.abort_transaction(30)
    produce(producer, topic, 2, value=lambda i: f"plain_{i}".encode())
    transactional_producer.begin_transaction()
    produce(transactional_producer, topic, 3, value=lambda i: f"committed_{i}".encode())
    transactional_producer.commit_transaction(30)
    # an aborted transaction at the end of the partition
    transactional_producer.begin_transaction()
    produce(transactional_producer, topic, 2, value=lambda i: f"tail_{i}".encode())
    transactional_producer.abort_transaction(30)

    credentials = local_credentials
    credentials.consumer_config = {"isolation.level": isolation_level}
    started = time.monotonic()
    finished, result = run_in_thread(
        lambda: pipeline.run(kafka_consumer(topic, credentials=credentials)), 30
    )
    assert finished and "error" not in result
    # the run ends at the markers, it does not wait for the batch timeout
    assert time.monotonic() - started < 20
    loaded = sorted(row[0] for row in table_rows(pipeline, topic, ["_kafka__data"]))
    expected = ["committed_0", "committed_1", "committed_2", "plain_0", "plain_1"]
    if isolation_level == "read_uncommitted":
        expected += ["aborted_0", "aborted_1", "aborted_2", "tail_0", "tail_1"]
    assert loaded == sorted(expected)
    pipeline.run(kafka_consumer(topic, credentials=credentials))
    assert last_row_count(pipeline, topic) == 0


def test_local_compacted_topic(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    topic = make_topic(
        **{
            "cleanup.policy": "compact",
            "segment.ms": "100",
            "min.cleanable.dirty.ratio": "0.001",
            "min.compaction.lag.ms": "0",
            "delete.retention.ms": "100",
        }
    )
    for update in range(3):
        for key in (b"a", b"b", b"c"):
            producer.produce(topic, f"{key.decode()}{update}".encode(), key)
        producer.flush()
        time.sleep(0.2)
    # a tombstone of `a` at the end of the compacted part
    producer.produce(topic, None, b"a")
    producer.flush()
    time.sleep(0.2)
    # rolls the segment so the cleaner compacts the ones before it
    produce(producer, topic, 1, key=b"filler")

    surviving: List[int] = []
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        consumer = Consumer(
            {
                "bootstrap.servers": LOCAL_KAFKA,
                "group.id": "dlt_compaction_check",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
            }
        )
        consumer.assign([TopicPartition(topic, 0, 0)])
        surviving = []
        while True:
            msg = consumer.poll(5)
            if msg is None or msg.error():
                break
            surviving.append(msg.offset())
        consumer.close()
        if len(surviving) < 6:
            break
        time.sleep(3)
    assert len(surviving) < 6, "the log was not compacted"

    assert_load_info(pipeline.run(kafka_consumer(topic, credentials=local_credentials)))
    assert [offset for _, offset in loaded_offsets(pipeline, topic)] == surviving
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    assert last_row_count(pipeline, topic) == 0


def test_local_log_append_time(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
) -> None:
    topic = make_topic(**{"message.timestamp.type": "LogAppendTime"})
    # the broker replaces the time the producer sets
    produce(producer, topic, 2, timestamp_ms=lambda i: BASE_TS_MS)
    before = datetime.now(timezone.utc)
    pipeline.run(
        kafka_consumer(
            topic,
            credentials=local_credentials,
            start_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    )
    rows = table_rows(pipeline, topic, ["_kafka__ts__type", "_kafka__ts__value"])
    assert len(rows) == 2
    for ts_type, ts_value in rows:
        assert ts_type == 2
        assert abs((ts_value - before).total_seconds()) < 60


def test_local_compression(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    local_credentials: KafkaCredentials,
) -> None:
    # the broker recompresses batches of another codec than the topic has
    topic = make_topic(**{"compression.type": "zstd"})
    compressed = Producer({"bootstrap.servers": LOCAL_KAFKA, "compression.type": "lz4"})
    produce(compressed, topic, 50, value=lambda i: f"value_{i}".encode() * 20)
    assert_load_info(pipeline.run(kafka_consumer(topic, credentials=local_credentials)))
    rows = table_rows(pipeline, topic, ["_kafka__data"])
    assert [row[0] for row in rows] == [f"value_{i}" * 20 for i in range(50)]


def test_local_large_message(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    local_credentials: KafkaCredentials,
) -> None:
    size = 5 * 1024 * 1024
    topic = make_topic(**{"max.message.bytes": str(2 * size)})
    large = Producer({"bootstrap.servers": LOCAL_KAFKA, "message.max.bytes": 2 * size})
    produce(large, topic, 3, value=lambda i: (b"x" * size if i == 1 else b"small"))
    assert_load_info(pipeline.run(kafka_consumer(topic, credentials=local_credentials)))
    with pipeline.sql_client() as client:
        lengths = client.execute_sql(
            f"SELECT length(_kafka__data) FROM {topic} ORDER BY _kafka__offset"
        )
    assert [row[0] for row in lengths] == [5, size, 5]


def test_local_partitions_added(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    kafka_admin: AdminClient,
    local_credentials: KafkaCredentials,
) -> None:
    from confluent_kafka.admin import NewPartitions

    topic = make_topic()
    produce(producer, topic, 2, partition=0)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    for future in kafka_admin.create_partitions([NewPartitions(topic, 3)]).values():
        future.result()
    # a new producer, the metadata of the other one does not have the new partitions yet
    fresh = Producer({"bootstrap.servers": LOCAL_KAFKA})
    deadline = time.monotonic() + 30
    while len(fresh.list_topics(topic, timeout=5).topics[topic].partitions) < 3:
        assert time.monotonic() < deadline, "the new partitions did not show up"
        time.sleep(1)
    produce(fresh, topic, 2, partition=2)
    produce(fresh, topic, 1, partition=0)
    pipeline.run(kafka_consumer(topic, credentials=local_credentials))
    # a new partition is read from its first message
    assert loaded_offsets(pipeline, topic) == [(0, 0), (0, 1), (0, 2), (2, 0), (2, 1)]


@pytest.mark.parametrize("on_type_change", ["strict", "skip"])
def test_local_type_change(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    local_registry: Any,
    local_registry_credentials: SchemaRegistryCredentials,
    on_type_change: Any,
) -> None:
    topic = make_topic()

    def schema(amount_type: str) -> Dict[str, Any]:
        return {
            "type": "record",
            "name": "Payment",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "amount", "type": amount_type},
            ],
        }

    def payments() -> Any:
        resource = kafka_consumer(
            topic,
            credentials=local_credentials,
            msg_processor=AvroProcessor(),
            schema_registry=local_registry_credentials,
            on_type_change=on_type_change,
        )
        if on_type_change == "skip":
            resource.apply_hints(schema_contract={"data_type": "freeze"})
        return resource

    produce_registry(
        producer,
        local_registry,
        topic,
        schema("int"),
        [{"id": 1, "amount": 2}],
        serializer="avro",
    )
    pipeline.run(payments())
    # int to double is a compatible Avro change, dlt never changes the destination column
    produce_registry(
        producer,
        local_registry,
        topic,
        schema("double"),
        [{"id": 2, "amount": 2.5}],
        serializer="avro",
    )
    with pytest.raises(PipelineStepFailed) as ex:
        pipeline.run(payments())
    expected = (
        IncompatibleColumnTypeException
        if on_type_change == "strict"
        else DataValidationError
    )
    error = next(e for e in exception_chain(ex.value) if isinstance(e, expected))
    assert "amount" in str(error)
    assert column_types(pipeline, topic)["amount"] == "bigint"
    assert table_rows(pipeline, topic, ["id", "amount"]) == [(1, 2)]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "table_per_schema", [False, True], ids=["one_table", "table_per_schema"]
)
def test_local_avro_references_and_union(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    local_registry: Any,
    local_registry_credentials: SchemaRegistryCredentials,
    table_per_schema: bool,
    backend: TBackend,
) -> None:
    from confluent_kafka.schema_registry import Schema, SchemaReference

    topic = make_topic()
    # the second field uses the named type that the first one references
    created = {
        "type": "record",
        "name": "Created",
        "namespace": "com.shop",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "billing", "type": "Address"},
            {"name": "shipping", "type": "Address"},
        ],
    }
    shipped = {
        "type": "record",
        "name": "Shipped",
        "namespace": "com.shop",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "carrier", "type": "string"},
        ],
    }
    address_ref = local_registry.register_schema_full_response(
        f"{topic}-address", Schema(json.dumps(ADDRESS_AVRO), "AVRO")
    )
    references = []
    for name, schema in (("Created", created), ("Shipped", shipped)):
        registered = local_registry.register_schema_full_response(
            f"{topic}-{name}",
            Schema(
                json.dumps(schema),
                "AVRO",
                references=[
                    SchemaReference(
                        "com.shop.Address", f"{topic}-address", address_ref.version
                    )
                ],
            ),
        )
        references.append(
            SchemaReference(f"com.shop.{name}", f"{topic}-{name}", registered.version)
        )
    # several event types in one topic: a union of references
    union = ["com.shop.Created", "com.shop.Shipped"]
    union_id = local_registry.register_schema(
        f"{topic}-value", Schema(json.dumps(union), "AVRO", references=references)
    )
    expanded = [
        {
            **created,
            "fields": [
                created["fields"][0],
                {"name": "billing", "type": ADDRESS_AVRO},
                created["fields"][2],
            ],
        },
        shipped,
    ]
    events = [
        (
            "com.shop.Created",
            {
                "id": 1,
                "billing": {"city": "Berlin", "zip": 1},
                "shipping": {"city": "Paris", "zip": 2},
            },
        ),
        ("com.shop.Shipped", {"id": 1, "carrier": "ups"}),
        (
            "com.shop.Created",
            {
                "id": 2,
                "billing": {"city": "Rome", "zip": 3},
                "shipping": {"city": "Oslo", "zip": 4},
            },
        ),
    ]
    for event in events:
        producer.produce(topic, avro_framed(union_id, expanded, event), b"k")
    producer.flush()
    assert_load_info(
        run_processor(
            pipeline,
            topic,
            local_credentials,
            AvroProcessor(
                backend=backend,
                table_per_schema=table_per_schema,
                table_name=lambda topic, name: name.rsplit(".", 1)[1].lower(),
            ),
            local_registry_credentials,
        )
    )
    if not table_per_schema:
        columns = ["id", "billing__city", "shipping__city", "carrier", "_kafka__offset"]
        assert table_rows(pipeline, topic, columns) == [
            (1, "Berlin", "Paris", None, 0),
            (1, None, None, "ups", 1),
            (2, "Rome", "Oslo", None, 2),
        ]
        return
    assert sorted(pipeline.default_schema.data_table_names()) == ["created", "shipped"]
    columns = ["id", "billing__city", "shipping__city", "_kafka__offset"]
    assert table_rows(pipeline, "created", columns) == [
        (1, "Berlin", "Paris", 0),
        (2, "Rome", "Oslo", 2),
    ]
    assert table_rows(pipeline, "shipped", ["id", "carrier", "_kafka__offset"]) == [
        (1, "ups", 1)
    ]


@pytest.mark.parametrize("backend", BACKENDS)
def test_local_avro_keys(
    pipeline: dlt.Pipeline,
    make_topic: MakeTopic,
    producer: Producer,
    local_credentials: KafkaCredentials,
    local_registry: Any,
    local_registry_credentials: SchemaRegistryCredentials,
    backend: TBackend,
) -> None:
    topic = make_topic()
    key_schema = {
        "type": "record",
        "name": "Key",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "region", "type": "string"},
        ],
    }
    key = {"id": 1, "region": "eu"}
    produce_registry(
        producer,
        local_registry,
        topic,
        ROW_AVRO,
        [{"v": "a"}],
        "avro",
        keys=[key],
        key_schema=key_schema,
    )
    # keys without a schema: a long key starts with a 0 byte and makes schema id 0
    produce_registry(
        producer,
        local_registry,
        topic,
        ROW_AVRO,
        [{"v": "b"}, {"v": "c"}, {"v": "d"}],
        "avro",
        keys=[(65).to_bytes(8, "big"), b"plain", None],
    )
    # a tombstone keeps its key
    produce_registry(
        producer,
        local_registry,
        topic,
        ROW_AVRO,
        [None],
        "avro",
        keys=[key],
        key_schema=key_schema,
    )
    run_processor(
        pipeline,
        topic,
        local_credentials,
        AvroProcessor(backend=backend),
        local_registry_credentials,
    )
    types = column_types(pipeline, topic)
    assert (types["_kafka__key__id"], types["_kafka__key__region"]) == (
        "bigint",
        "text",
    )
    rows = table_rows(
        pipeline,
        topic,
        ["v", "_kafka__key__id", "_kafka__key__region", "_kafka__key", "_kafka_msg_id"],
    )
    # the long key loads as text, inserts into duckdb drop its 0 characters, parquet keeps them
    assert [row[:3] + (row[3] and row[3].lstrip("\x00"),) for row in rows] == [
        ("a", 1, "eu", None),
        ("b", None, None, "A"),
        ("c", None, None, "plain"),
        ("d", None, None, None),
        (None, 1, "eu", None),
    ]
    # messages of a key share the id
    assert rows[0][4] == rows[4][4]
