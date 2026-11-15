import base64
import json
import sys
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List
from uuid import uuid4

import dlt
import pytest
from dlt.common import Decimal
from dlt.common.exceptions import MissingDependencyException
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)
from dlt.pipeline.exceptions import PipelineStepFailed

from sources.kafka import JsonProcessor, MessageDecodeError, kafka_consumer
from sources.kafka.helpers.processors import resolve_backend

from tests.kafka.utils import (
    BACKENDS,
    FakeConsumer,
    MockRegistry,
    UseFakeConsumer,
    column_types,
    exception_chain,
    guid_headers,
    loaded_offsets,
    message,
    record_partition,
    register,
    run_processor,
    skipped_offsets,
    table_rows,
)

TOPIC = "orders.v1-json"
TABLE = "orders_v1_json"

ORDER_SCHEMA = {
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
            "properties": {"name": {"type": "string"}, "vip": {"type": "boolean"}},
        },
        "tags": {"type": "array", "items": {"type": "string"}},
    },
}
ORDERS = [
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
    None,
]
VALUE_COLUMNS = [
    "id",
    "amount",
    "created",
    "day",
    "note",
    "customer__name",
    "customer__vip",
]


@pytest.mark.parametrize("framing", ["prefix", "header"])
def test_backends_load_registry_types(
    use_fake_consumer: UseFakeConsumer, mock_registry: MockRegistry, framing: str
) -> None:
    registry, registry_credentials = mock_registry
    registered = register(registry, f"{TOPIC}-value", ORDER_SCHEMA)
    loaded = []
    for backend in BACKENDS:
        schema_ids = [registered.schema_id] * len(ORDERS)
        partition = record_partition(
            TOPIC, ORDERS, schema_ids=schema_ids if framing == "prefix" else None
        )
        if framing == "header":
            for record in partition.records:
                record.set_headers(guid_headers(registered.guid))
        credentials = use_fake_consumer(FakeConsumer({TOPIC: [partition]}))
        pipeline = dlt.pipeline(
            f"json_{uuid4().hex[:8]}", destination="duckdb", dev_mode=True
        )
        run_processor(
            pipeline,
            TOPIC,
            credentials,
            JsonProcessor(backend=backend, strict=True),
            registry_credentials,
        )
        loaded.append(
            (
                pipeline.default_schema.data_table_names(),
                column_types(pipeline, TABLE),
                table_rows(
                    pipeline, TABLE, VALUE_COLUMNS + ["_kafka__offset", "_kafka__key"]
                ),
            )
        )
    python, pyarrow = loaded
    tables, types, rows = python
    # lists are `json` from the schema, no nested tables
    assert tables == [TABLE]
    assert {name: types[name] for name in VALUE_COLUMNS + ["tags"]} == {
        "id": "bigint",
        "amount": "double",
        "created": "timestamp",
        "day": "date",
        "note": "text",
        "customer__name": "text",
        "customer__vip": "bool",
        "tags": "json",
    }
    assert rows == [
        (
            1,
            1.5,
            datetime(2026, 1, 1, 8, tzinfo=timezone.utc),
            date(2026, 1, 1),
            "first",
            "ann",
            True,
            0,
            "key_0",
        ),
        (
            2,
            2.0,
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            date(2026, 1, 2),
            None,
            "bob",
            False,
            1,
            "key_1",
        ),
        # a tombstone loads the metadata only
        (None, None, None, None, None, None, None, 2, "key_2"),
    ]
    assert pyarrow == python


def test_backends_without_schema(use_fake_consumer: UseFakeConsumer) -> None:
    values = [
        {"id": 1, "name": "a", "nested": {"x": 1}},
        {"id": 2, "extra": True},
        None,
    ]
    loaded = []
    for backend in BACKENDS:
        # a framed value without a registry loads without the framing
        partition = record_partition(TOPIC, values, schema_ids=[None, 7, 7])
        credentials = use_fake_consumer(FakeConsumer({TOPIC: [partition]}))
        pipeline = dlt.pipeline(
            f"json_{uuid4().hex[:8]}", destination="duckdb", dev_mode=True
        )
        run_processor(pipeline, TOPIC, credentials, JsonProcessor(backend=backend))
        loaded.append(
            table_rows(
                pipeline, TABLE, ["id", "name", "nested__x", "extra", "_kafka__offset"]
            )
        )
    assert (
        loaded[0]
        == loaded[1]
        == [
            (1, "a", 1, None, 0),
            (2, None, None, True, 1),
            (None, None, None, None, 2),
        ]
    )


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("on_decode_error", ["raise", "skip"])
def test_decode_errors(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    kafka_warnings: List[str],
    backend: str,
    on_decode_error: Any,
) -> None:
    partition = record_partition(
        TOPIC,
        [{"id": 0}, b"{not json", b"[1, 2]", {"id": 3}, {"id": 4}, b"", {"id": 6}],
    )
    partition.records[4] = message(TOPIC, 0, 4, key=b"\xff\x00", value=b'{"id": 4}')
    credentials = use_fake_consumer(FakeConsumer({TOPIC: [partition]}))
    processor = JsonProcessor(backend=backend, on_decode_error=on_decode_error)  # type: ignore[arg-type]
    if on_decode_error == "raise":
        with pytest.raises(PipelineStepFailed) as ex:
            run_processor(pipeline, TOPIC, credentials, processor)
        error = next(
            e for e in exception_chain(ex.value) if isinstance(e, MessageDecodeError)
        )
        assert (error.topic, error.partition, error.offset) == (TOPIC, 0, 1)
        return
    run_processor(pipeline, TOPIC, credentials, processor)
    assert loaded_offsets(pipeline, TABLE) == [(0, 0), (0, 3), (0, 6)]
    assert sorted(skipped_offsets(kafka_warnings)) == [1, 2, 4, 5]


@pytest.mark.parametrize("backend", BACKENDS)
def test_strict(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    backend: str,
) -> None:
    registry, registry_credentials = mock_registry
    schema_id = register(registry, f"{TOPIC}-value", ORDER_SCHEMA).schema_id
    partition = record_partition(
        TOPIC,
        [{"id": 1}, {"id": 2, "unknown": 1}, {"id": 4}],
        schema_ids=[schema_id, schema_id, None],
    )
    credentials = use_fake_consumer(FakeConsumer({TOPIC: [partition]}))
    processor = JsonProcessor(backend=backend, strict=True, on_decode_error="skip")  # type: ignore[arg-type]
    run_processor(pipeline, TOPIC, credentials, processor, registry_credentials)
    # a field outside the schema and a message without a schema id are skipped
    assert loaded_offsets(pipeline, TABLE) == [(0, 0)]

    # a topic without a subject fails before reading
    with pytest.raises(PipelineStepFailed) as ex:
        run_processor(pipeline, [TOPIC, "other"], credentials, JsonProcessor(backend=backend, strict=True), registry_credentials)  # type: ignore[arg-type]
    assert "['other'] have no schema" in str(ex.value)
    with pytest.raises(PipelineStepFailed, match="needs a Schema Registry"):
        run_processor(pipeline, TOPIC, credentials, JsonProcessor(strict=True))


def test_without_pyarrow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    assert resolve_backend("auto", "JsonProcessor") == "python"
    with pytest.raises(MissingDependencyException):
        JsonProcessor(backend="pyarrow")
    processor = JsonProcessor()
    processor.prepare([TOPIC], None, SnakeCaseNamingConvention())
    items = processor(TOPIC, None, None, record_partition(TOPIC, [{"id": 1}]).records)  # type: ignore[arg-type]
    assert items[0].data[0]["id"] == 1


def test_pyarrow_keeps_types(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    kafka_warnings: List[str],
) -> None:
    values = [
        {"n": 1, "tags": ["a"]},
        {"n": 2},
        {"n": 3.5, "tags": []},
        {"n": 4, "tags": ["b"]},
    ]
    partition = record_partition(TOPIC, values)
    pipeline.run(
        kafka_consumer(
            TOPIC,
            credentials=use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
            msg_processor=JsonProcessor(backend="pyarrow", on_decode_error="skip"),
            batch_size=1,
        )
    )
    # the type of the first batch stays, dlt can't load a type change within a load
    assert table_rows(pipeline, TABLE, ["n", "tags"]) == [
        (1, '["a"]'),
        (2, None),
        (4, '["b"]'),
    ]
    assert column_types(pipeline, TABLE)["n"] == "bigint"
    assert any("offset 2 " in warning for warning in kafka_warnings)


def connect_value(schema: Dict[str, Any], payload: Any) -> bytes:
    """A value as the Kafka Connect `JsonConverter` writes it, schema first"""
    return json.dumps(
        {"schema": schema, "payload": payload}, separators=(",", ":")
    ).encode()


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode()


CONNECT_SCHEMA: Dict[str, Any] = {
    "type": "struct",
    "name": "dbserver1.inventory.orders.Value",
    "optional": False,
    "fields": [
        {"type": "int32", "optional": False, "field": "id"},
        {
            "type": "bytes",
            "optional": True,
            "name": "org.apache.kafka.connect.data.Decimal",
            "parameters": {"scale": "2", "connect.decimal.precision": "10"},
            "field": "price",
        },
        {
            "type": "int64",
            "optional": True,
            "name": "io.debezium.time.MicroTimestamp",
            "field": "updated",
        },
        {
            "type": "int32",
            "optional": True,
            "name": "io.debezium.time.Date",
            "field": "day",
        },
        {
            "type": "int64",
            "optional": True,
            "name": "io.debezium.time.MicroTime",
            "field": "at",
        },
        {
            "type": "string",
            "optional": True,
            "name": "io.debezium.time.ZonedTimestamp",
            "field": "zoned",
        },
        {
            "type": "struct",
            "optional": True,
            "name": "io.debezium.data.VariableScaleDecimal",
            "fields": [
                {"type": "int32", "field": "scale"},
                {"type": "bytes", "field": "value"},
            ],
            "field": "amount",
        },
        {"type": "bytes", "optional": True, "field": "blob"},
        {
            "type": "array",
            "items": {"type": "string"},
            "optional": True,
            "field": "tags",
        },
        {
            "type": "struct",
            "optional": True,
            "field": "address",
            "fields": [{"type": "string", "optional": True, "field": "city"}],
        },
    ],
}


@pytest.mark.parametrize("backend", BACKENDS)
def test_kafka_connect_envelope(
    pipeline: dlt.Pipeline, use_fake_consumer: UseFakeConsumer, backend: str
) -> None:
    updated = datetime(2026, 1, 1, 10, 0, 0, 123456, tzinfo=timezone.utc)
    payload = {
        "id": 1,
        "price": b64((12345).to_bytes(2, "big", signed=True)),
        "updated": (updated - datetime(1970, 1, 1, tzinfo=timezone.utc))
        // timedelta(microseconds=1),
        "day": (date(2026, 1, 1) - date(1970, 1, 1)).days,
        "at": 10 * 3600 * 1_000_000,
        "zoned": "2026-01-01T10:00:00+02:00",
        "amount": {
            "scale": 3,
            "value": b64((-1234567).to_bytes(4, "big", signed=True)),
        },
        "blob": b64(b"\x00\x01"),
        "tags": ["x"],
        "address": {"city": "Berlin"},
    }
    values = [
        connect_value(CONNECT_SCHEMA, payload),
        connect_value(CONNECT_SCHEMA, None),
        None,
    ]
    credentials = use_fake_consumer(
        FakeConsumer({TOPIC: [record_partition(TOPIC, values)]})
    )
    run_processor(pipeline, TOPIC, credentials, JsonProcessor(backend=backend, envelope="kafka_connect"))  # type: ignore[arg-type]
    types = column_types(pipeline, TABLE)
    names = [
        "id",
        "price",
        "updated",
        "day",
        "at",
        "zoned",
        "amount",
        "blob",
        "address__city",
    ]
    assert {name: types[name] for name in names + ["tags"]} == {
        "id": "bigint",
        "price": "decimal",
        "updated": "timestamp",
        "day": "date",
        "at": "time",
        "zoned": "timestamp",
        "amount": "text",
        "blob": "binary",
        "address__city": "text",
        "tags": "json",
    }
    rows = table_rows(pipeline, TABLE, names)
    assert rows[0] == (
        1,
        Decimal("123.45"),
        updated,
        date(2026, 1, 1),
        time(10, 0),
        datetime(2026, 1, 1, 8, tzinfo=timezone.utc),
        "-1234.567",
        b"\x00\x01",
        "Berlin",
    )
    # a null payload and a tombstone load the metadata only
    assert rows[1:] == [(None,) * len(names)] * 2


def order_schema(properties: Dict[str, Any], title: str = "Order") -> Dict[str, Any]:
    return {"title": title, "type": "object", "properties": properties}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "subject,expected_hints",
    [
        (f"{TOPIC}-value", [["amount", "id", "unset"]]),
        ("other-value", [["id", "unset"], ["amount", "id", "unset"]]),
    ],
    ids=["topic_subject", "other_subject"],
)
def test_registry_hints(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    emitted_hints: List[Dict[str, Any]],
    backend: Any,
    subject: str,
    expected_hints: List[List[str]],
) -> None:
    registry, registry_credentials = mock_registry
    v1_properties = {"id": {"type": "integer"}, "unset": {"type": "string"}}
    v1 = register(registry, subject, order_schema(v1_properties)).schema_id
    v2_properties = {**v1_properties, "amount": {"type": "number"}}
    v2 = register(registry, subject, order_schema(v2_properties)).schema_id
    # writers of v1 and v2 interleave
    partition = record_partition(
        TOPIC,
        [{"id": 1}, {"id": 2}, {"id": 3, "amount": 1}, {"id": 4}],
        schema_ids=[v1, v1, v2, v1],
    )
    resource = kafka_consumer(
        TOPIC,
        credentials=use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
        msg_processor=JsonProcessor(backend=backend),
        schema_registry=registry_credentials,
    )
    resource.apply_hints(columns={"id": {"data_type": "text"}})
    pipeline.run(resource)
    # the latest schema of `<topic>-value` before the first message, else each schema that adds
    # columns
    assert [sorted(hints["columns"]) for hints in emitted_hints] == expected_hints
    types = column_types(pipeline, TABLE)
    # columns of the schema without values, columns the user applied win
    assert (types["unset"], types["id"], types["amount"]) == ("text", "text", "double")
    assert table_rows(pipeline, TABLE, ["id", "amount"]) == [
        ("1", None),
        ("2", None),
        ("3", 1.0),
        ("4", None),
    ]


def test_no_hints_without_registry(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    emitted_hints: List[Dict[str, Any]],
) -> None:
    partition = record_partition(TOPIC, [{"id": 1}], schema_ids=[3])
    run_processor(
        pipeline,
        TOPIC,
        use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
        JsonProcessor(),
    )
    assert emitted_hints == []
    assert table_rows(pipeline, TABLE, ["id"]) == [(1,)]


@pytest.mark.parametrize("backend", BACKENDS)
def test_table_per_schema(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    backend: Any,
) -> None:
    registry, registry_credentials = mock_registry
    created = register(
        registry,
        "OrderCreated",
        order_schema({"id": {"type": "integer"}}, "OrderCreated"),
    ).schema_id
    shipped = register(
        registry,
        "OrderShipped",
        order_schema({"carrier": {"type": "string"}}, "OrderShipped"),
    ).schema_id
    partition = record_partition(
        TOPIC,
        [{"id": 1}, {"carrier": "ups"}, {"id": 2}],
        schema_ids=[created, shipped, created],
    )
    run_processor(
        pipeline,
        TOPIC,
        use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
        JsonProcessor(
            backend=backend,
            table_per_schema=True,
            table_name=lambda topic, schema: schema.lower(),
        ),
        registry_credentials,
    )
    assert sorted(pipeline.default_schema.data_table_names()) == [
        "ordercreated",
        "ordershipped",
    ]
    assert table_rows(pipeline, "ordercreated", ["id", "_kafka__offset"]) == [
        (1, 0),
        (2, 2),
    ]
    assert table_rows(pipeline, "ordershipped", ["carrier", "_kafka__offset"]) == [
        ("ups", 1)
    ]


@pytest.mark.parametrize("backend", BACKENDS)
def test_naming_convention_paths(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    monkeypatch: pytest.MonkeyPatch,
    backend: Any,
) -> None:
    # hints and data columns of nested fields use the path separator of the naming convention
    monkeypatch.setenv("SCHEMA__NAMING", "direct")
    # direct naming keeps the dot of a topic name, and dlt cannot use it in file names
    topic = "orders"
    registry, registry_credentials = mock_registry
    properties = {
        "customer": {"type": "object", "properties": {"Name": {"type": "string"}}}
    }
    schema_id = register(registry, f"{topic}-value", order_schema(properties)).schema_id
    partition = record_partition(
        topic, [{"customer": {"Name": "ann"}}], schema_ids=[schema_id]
    )
    run_processor(
        pipeline,
        topic,
        use_fake_consumer(FakeConsumer({topic: [partition]})),
        JsonProcessor(backend=backend),
        registry_credentials,
    )
    columns = pipeline.default_schema.get_table_columns(topic)
    assert columns["customer▶Name"]["data_type"] == "text"
    assert "customer__Name" not in columns
    assert "_kafka▶offset" in columns
