import io
import json
import os
import sys
import threading
from datetime import datetime, timezone
from uuid import UUID
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import dlt
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    KafkaError,
    Message,
    Producer,
    TopicPartition,
)

from sources.kafka import KafkaCredentials, SchemaRegistryCredentials, kafka_consumer

LOCAL_KAFKA = "localhost:9092"
LOCAL_KAFKA_SASL = "localhost:9094"
LOCAL_KAFKA_MTLS = "localhost:9095"
LOCAL_SCHEMA_REGISTRY = "http://localhost:8081"
CERTS_DIR = os.path.join(os.path.dirname(__file__), "certs")
SCRAM_USER = ("dlt_scram", "scram-secret")
BASE_TS_MS = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
BACKENDS = ["python", "pyarrow"]

MakeTopic = Callable[..., str]
MockRegistry = Tuple[Any, SchemaRegistryCredentials]
UseFakeConsumer = Callable[["FakeConsumer"], KafkaCredentials]


def as_datetime(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc)


class SeededMessage(NamedTuple):
    partition: int
    offset: int
    ts_ms: int


def message(
    topic: str,
    partition: int,
    offset: int,
    key: Optional[bytes] = b"k",
    value: Optional[bytes] = b"v",
    timestamp: Tuple[int, int] = (TIMESTAMP_CREATE_TIME, BASE_TS_MS),
    headers: Optional[List[Tuple[str, bytes]]] = None,
    error: Optional[KafkaError] = None,
) -> Message:
    return Message(
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        value=value,
        timestamp=timestamp,
        headers=headers,
        error=error,
    )


class FakePartition:
    """Log of one partition: a `None` record is a transaction marker that occupies an offset"""

    def __init__(self, records: List[Optional[Message]], low: int = 0) -> None:
        self.records = records
        self.low = low

    @property
    def high(self) -> int:
        return self.low + len(self.records)

    def record(self, offset: int) -> Optional[Message]:
        return self.records[offset - self.low]


class FakeTopicMetadata:
    def __init__(self, partitions: Sequence[int]) -> None:
        self.partitions = {p: None for p in partitions}
        self.error: Optional[KafkaError] = None


class FakeClusterMetadata:
    def __init__(self, topics: Dict[str, FakeTopicMetadata]) -> None:
        self.topics = topics


class FakeConsumer:
    """In-memory consumer with the `confluent_kafka.Consumer` calls the source makes.

    Assigned offsets out of the log range reset to the log start, like
    `auto.offset.reset=earliest`. `live` partitions get a new message on every `consume` call
    until `live_limit` messages were added.
    """

    def __init__(
        self,
        topics: Dict[str, List[FakePartition]],
        partition_eof: bool = True,
        live: Iterable[Tuple[str, int]] = (),
        live_limit: int = 2000,
        errors: Iterable[KafkaError] = (),
    ) -> None:
        self.topics = topics
        self.partition_eof = partition_eof
        self.live = list(live)
        self.live_limit = live_limit
        self.errors = list(errors)
        self.positions: Dict[Tuple[str, int], int] = {}
        self.eof_sent: Set[Tuple[str, int]] = set()
        self.paused: Set[Tuple[str, int]] = set()
        self.consume_calls = 0
        self.closed = False

    def list_topics(self, timeout: float = -1) -> FakeClusterMetadata:
        return FakeClusterMetadata(
            {
                name: FakeTopicMetadata(range(len(parts)))
                for name, parts in self.topics.items()
            }
        )

    def get_watermark_offsets(
        self,
        partition: TopicPartition,
        timeout: Optional[float] = None,
        cached: bool = False,
    ) -> Tuple[int, int]:
        part = self.topics[partition.topic][partition.partition]
        return part.low, part.high

    def offsets_for_times(
        self, partitions: List[TopicPartition], timeout: Optional[float] = None
    ) -> List[TopicPartition]:
        result = []
        for tp in partitions:
            part = self.topics[tp.topic][tp.partition]
            found = -1
            for offset in range(part.low, part.high):
                msg = part.record(offset)
                if msg is not None and msg.timestamp()[1] >= tp.offset:
                    found = offset
                    break
            result.append(TopicPartition(tp.topic, tp.partition, found))
        return result

    def assign(self, partitions: List[TopicPartition]) -> None:
        for tp in partitions:
            part = self.topics[tp.topic][tp.partition]
            offset = tp.offset if part.low <= tp.offset <= part.high else part.low
            self.positions[(tp.topic, tp.partition)] = offset

    def position(self, partitions: List[TopicPartition]) -> List[TopicPartition]:
        return [
            TopicPartition(
                tp.topic, tp.partition, self.positions[(tp.topic, tp.partition)]
            )
            for tp in partitions
        ]

    def pause(self, partitions: List[TopicPartition]) -> None:
        self.paused.update((tp.topic, tp.partition) for tp in partitions)

    def consume(self, num_messages: int = 1, timeout: float = -1) -> List[Message]:
        if self.closed:
            raise RuntimeError("Consumer closed")
        self.consume_calls += 1
        for topic, partition in self.live:
            part = self.topics[topic][partition]
            if len(part.records) < self.live_limit:
                part.records.append(make_message(topic, partition, part.high))
        batch: List[Message] = [
            message("", -1, -1, error=error) for error in self.errors
        ]
        self.errors = []
        for (topic, partition), position in list(self.positions.items()):
            if (topic, partition) in self.paused:
                continue
            part = self.topics[topic][partition]
            while position < part.high and len(batch) < num_messages:
                msg = part.record(position)
                position += 1
                if msg is not None:
                    batch.append(msg)
            self.positions[(topic, partition)] = position
            if (
                self.partition_eof
                and position == part.high
                and (topic, partition) not in self.eof_sent
                and len(batch) < num_messages
            ):
                self.eof_sent.add((topic, partition))
                batch.append(
                    message(
                        topic,
                        partition,
                        position,
                        error=KafkaError(KafkaError._PARTITION_EOF),
                    )
                )
        return batch

    def close(self) -> None:
        self.closed = True


def make_message(
    topic: str,
    partition: int,
    offset: int,
    key: Optional[bytes] = b"k",
    value: Optional[bytes] = None,
    ts_ms: Optional[int] = None,
) -> Message:
    return message(
        topic,
        partition,
        offset,
        key=key,
        value=value if value is not None else f"value_{offset}".encode(),
        timestamp=(
            TIMESTAMP_CREATE_TIME,
            BASE_TS_MS + offset * 1000 if ts_ms is None else ts_ms,
        ),
    )


def make_partition(
    topic: str, partition: int, count: int, low: int = 0, markers: Iterable[int] = ()
) -> FakePartition:
    """`count` messages from offset `low`, transaction markers at `markers` offsets"""
    marker_offsets = set(markers)
    return FakePartition(
        [
            None if offset in marker_offsets else make_message(topic, partition, offset)
            for offset in range(low, low + count)
        ],
        low=low,
    )


def framed(schema_id: int, value: Any) -> bytes:
    """JSON value with the Confluent framing: a 0 byte and the 4 bytes of `schema_id`"""
    return b"\x00" + schema_id.to_bytes(4, "big") + json.dumps(value).encode()


def guid_headers(guid: str) -> List[Tuple[str, bytes]]:
    """`__value_schema_id` header of a schema GUID"""
    return [("__value_schema_id", b"\x01" + UUID(guid).bytes)]


def avro_body(schema: Any, value: Any) -> bytes:
    """Avro binary encoding of `value`"""
    import fastavro

    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, fastavro.parse_schema(schema), value)
    return buffer.getvalue()


def avro_framed(schema_id: int, schema: Any, value: Any) -> bytes:
    """Avro `value` with the Confluent framing of `schema_id`"""
    return b"\x00" + schema_id.to_bytes(4, "big") + avro_body(schema, value)


def record_partition(
    topic: str,
    values: Sequence[Any],
    schema_ids: Optional[Sequence[Optional[int]]] = None,
    keys: Optional[Sequence[Optional[bytes]]] = None,
    headers: Optional[Sequence[Any]] = None,
) -> FakePartition:
    """Messages of partition 0 with keys `key_<offset>` unless given. A value that is not bytes
    is JSON, framed with its schema id"""
    records: List[Optional[Message]] = []
    for offset, value in enumerate(values):
        schema_id = schema_ids[offset] if schema_ids is not None else None
        if isinstance(value, bytes) or value is None:
            data = value
        elif schema_id is None:
            data = json.dumps(value).encode()
        else:
            data = framed(schema_id, value)
        records.append(
            message(
                topic,
                0,
                offset,
                key=keys[offset] if keys is not None else f"key_{offset}".encode(),
                value=data,
                timestamp=(TIMESTAMP_CREATE_TIME, BASE_TS_MS + offset * 1000),
                headers=headers[offset] if headers is not None else None,
            )
        )
    return FakePartition(records)


def register(
    registry: Any, subject: str, schema: Any, schema_type: str = "JSON", **kwargs: Any
) -> Any:
    """Registers `schema` under `subject`, returns the `RegisteredSchema`"""
    from confluent_kafka.schema_registry import Schema

    return registry.register_schema_full_response(
        subject, Schema(json.dumps(schema), schema_type, **kwargs)
    )


def run_processor(
    pipeline: dlt.Pipeline,
    topic: str,
    credentials: KafkaCredentials,
    processor: Any,
    registry_credentials: Optional[SchemaRegistryCredentials] = None,
) -> Any:
    return pipeline.run(
        kafka_consumer(
            topic,
            credentials=credentials,
            msg_processor=processor,
            schema_registry=registry_credentials,
        )
    )


def produce_registry(
    producer: Producer,
    registry: Any,
    topic: str,
    schema: Any,
    values: Sequence[Any],
    serializer: str = "json",
    keys: Optional[Sequence[Any]] = None,
    key_schema: Any = None,
    header: bool = False,
) -> None:
    """Produces `values` with a Schema Registry serializer, `json` or `avro`. A `None` value is a
    tombstone. Keys are serialized with `key_schema` when given."""
    from confluent_kafka.schema_registry import header_schema_id_serializer
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.schema_registry.json_schema import JSONSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    conf = {"schema.id.serializer": header_schema_id_serializer} if header else {}
    value_serializer: Any
    if serializer == "avro":
        value_serializer = AvroSerializer(registry, json.dumps(schema), conf=conf)
    else:
        value_serializer = JSONSerializer(json.dumps(schema), registry, conf=conf)
    key_serializer = (
        AvroSerializer(registry, json.dumps(key_schema)) if key_schema else None
    )
    for i, value in enumerate(values):
        context = SerializationContext(topic, MessageField.VALUE, headers=[])
        data = None if value is None else value_serializer(value, context)
        if key_serializer is not None:
            key = key_serializer(keys[i], SerializationContext(topic, MessageField.KEY))  # type: ignore[index]
        else:
            key = keys[i] if keys else f"key_{i}".encode()
        producer.produce(topic, data, key, headers=context.headers or None)
    producer.flush()


def skipped_offsets(warnings: Sequence[str]) -> List[int]:
    """Offsets of the messages that the warnings of `on_decode_error="skip"` name"""
    return [
        int(warning.split("offset ")[1].split(" ")[0])
        for warning in warnings
        if warning.startswith("Skipped")
    ]


def produce(
    producer: Producer,
    topic: str,
    count: int,
    partition: Optional[int] = None,
    key: Optional[bytes] = b"k",
    value: Callable[[int], Optional[bytes]] = lambda i: f"value_{i}".encode(),
    timestamp_ms: Optional[Callable[[int], int]] = None,
) -> None:
    for i in range(count):
        kwargs: Dict[str, Any] = {}
        if partition is not None:
            kwargs["partition"] = partition
        if timestamp_ms is not None:
            kwargs["timestamp"] = timestamp_ms(i)
        producer.produce(topic, value(i), key, **kwargs)
    producer.flush()


def table_rows(
    pipeline: dlt.Pipeline, table_name: str, columns: Sequence[str]
) -> List[Tuple[Any, ...]]:
    """`columns` of `table_name` ordered by partition and offset"""
    with pipeline.sql_client() as client:
        return [
            tuple(row)
            for row in client.execute_sql(
                f"SELECT {', '.join(columns)} FROM {table_name}"
                " ORDER BY _kafka__partition, _kafka__offset"
            )
        ]


def column_types(pipeline: dlt.Pipeline, table_name: str) -> Dict[str, Any]:
    """Data types of the columns of `table_name` without the dlt columns"""
    return {
        name: column.get("data_type")
        for name, column in pipeline.default_schema.get_table_columns(
            table_name
        ).items()
        if not name.startswith("_dlt")
    }


def loaded_offsets(pipeline: dlt.Pipeline, table_name: str) -> List[Tuple[int, int]]:
    """Sorted (partition, offset) pairs loaded into `table_name`"""
    with pipeline.sql_client() as client:
        if not client.has_dataset():
            return []
        try:
            rows = client.execute_sql(
                f"SELECT _kafka__partition, _kafka__offset FROM {table_name}"
            )
        except Exception:
            return []
    return sorted((int(p), int(o)) for p, o in rows)


def last_row_count(pipeline: dlt.Pipeline, table_name: str) -> int:
    """Rows of `table_name` that the last run of `pipeline` normalized"""
    return pipeline.last_trace.last_normalize_info.row_counts.get(table_name, 0)


def stored_offsets(pipeline: dlt.Pipeline) -> Dict[str, Dict[str, int]]:
    sources = pipeline.state.get("sources", {})
    resources = sources.get(pipeline.default_schema_name, {}).get("resources", {})
    return resources.get("kafka_messages", {}).get("offsets", {})  # type: ignore[no-any-return]


def run_in_thread(fn: Callable[[], Any], timeout: float) -> Tuple[bool, Dict[str, Any]]:
    """Runs `fn` in a daemon thread so a hang fails the test instead of the test session"""
    result: Dict[str, Any] = {}

    def target() -> None:
        try:
            result["value"] = fn()
        except Exception as ex:
            result["error"] = ex

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    return not thread.is_alive(), result


def raised_by(fn: Callable[[], Any]) -> Any:
    """What `fn` raises, `None` if nothing.

    A raised `confluent_kafka.KafkaError` is not a `BaseException`: only a bare `except` catches it
    and `pytest.raises` reports such a test as passed.
    """
    try:
        fn()
    except:  # noqa: E722,B001
        return sys.exc_info()[1]
    return None


def exception_chain(ex: BaseException) -> List[BaseException]:
    chain = []
    current: Any = ex
    while current is not None:
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


def read_cert(name: str) -> str:
    with open(os.path.join(CERTS_DIR, name), encoding="utf-8") as f:
        return f.read()


def auth_credentials(auth: str) -> KafkaCredentials:
    """Credentials of the SASL and MTLS listeners of the local Kafka"""
    fields: Dict[str, Any]
    if auth == "plain":
        fields = dict(
            bootstrap_servers=LOCAL_KAFKA_SASL,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanisms="PLAIN",
            sasl_username="dlt",
            sasl_password="dlt-secret",
        )
    elif auth == "scram":
        fields = dict(
            bootstrap_servers=LOCAL_KAFKA_SASL,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanisms="SCRAM-SHA-512",
            sasl_username=SCRAM_USER[0],
            sasl_password=SCRAM_USER[1],
        )
    elif auth == "mtls_pem":
        fields = dict(
            bootstrap_servers=LOCAL_KAFKA_MTLS,
            security_protocol="SSL",
            ssl_ca_pem=read_cert("ca.pem"),
            ssl_certificate_pem=read_cert("client.crt"),
            ssl_key_pem=read_cert("client.pk8"),
        )
    elif auth == "mtls_files":
        fields = dict(
            bootstrap_servers=LOCAL_KAFKA_MTLS,
            security_protocol="SSL",
            ssl_ca_location=os.path.join(CERTS_DIR, "ca.pem"),
            ssl_certificate_location=os.path.join(CERTS_DIR, "client.crt"),
            ssl_key_location=os.path.join(CERTS_DIR, "client.key"),
        )
    else:
        raise ValueError(auth)
    credentials = KafkaCredentials(**fields)
    credentials.resolve()
    return credentials
