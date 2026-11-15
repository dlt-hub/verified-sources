import json
import sys
from datetime import date, datetime, time, timezone
from functools import partial
from typing import Any, Dict, List
from uuid import uuid4

import dlt
import pytest
from dlt.common import Decimal
from dlt.common.exceptions import MissingDependencyException
from dlt.pipeline.exceptions import PipelineStepFailed

from sources.kafka import AvroProcessor, MessageDecodeError, kafka_consumer

from tests.kafka.utils import (
    BACKENDS,
    FakeConsumer,
    MockRegistry,
    UseFakeConsumer,
    avro_body,
    avro_framed,
    column_types,
    exception_chain,
    framed,
    guid_headers,
    loaded_offsets,
    record_partition,
    register,
    run_processor,
    skipped_offsets,
    table_rows,
)

TOPIC = "orders.v1-avro"
register_avro = partial(register, schema_type="AVRO")
TABLE = "orders_v1_avro"

ADDRESS = {
    "type": "record",
    "name": "Address",
    "namespace": "com.shop",
    "fields": [{"name": "city", "type": "string"}, {"name": "zip", "type": "int"}],
}
ORDER_SCHEMA = {
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
        {"name": "nanos", "type": {"type": "long", "logicalType": "timestamp-nanos"}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "at", "type": {"type": "int", "logicalType": "time-millis"}},
        {
            "name": "status",
            "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]},
        },
        {"name": "hash", "type": {"type": "fixed", "name": "Hash", "size": 2}},
        {"name": "note", "type": ["null", "string"]},
        {"name": "billing", "type": ["null", ADDRESS]},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "attributes", "type": {"type": "map", "values": "long"}},
        {"name": "score", "type": ["null", "long", "string"]},
        {
            "name": "updated",
            "type": {"type": "long", "connect.name": "io.debezium.time.MicroTimestamp"},
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
}
ORDERS = [
    {
        "id": 1,
        "orderId": "00000000-0000-0000-0000-000000000001",
        "amount": Decimal("12.34"),
        "created": datetime(2026, 1, 1, 8, tzinfo=timezone.utc),
        "local": datetime(2026, 1, 1, 9, 30),
        "nanos": 1767225600123456789,
        "day": date(2026, 1, 1),
        "at": time(10, 11, 12),
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
        "at": time(0),
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
]
VALUE_COLUMNS = [
    "id",
    "order_id",
    "amount",
    "created",
    "local",
    "nanos",
    "day",
    "at",
    "status",
    "hash",
    "note",
    "billing__city",
    "billing__zip",
    "tags",
    "attributes",
    "score",
    "updated",
    "price",
]


@pytest.mark.parametrize("framing", ["prefix", "header"])
def test_backends_load_registry_types(
    use_fake_consumer: UseFakeConsumer, mock_registry: MockRegistry, framing: str
) -> None:
    registry, registry_credentials = mock_registry
    registered = register_avro(registry, f"{TOPIC}-value", ORDER_SCHEMA)
    if framing == "prefix":
        values = [avro_framed(registered.schema_id, ORDER_SCHEMA, v) for v in ORDERS]
        headers = None
    else:
        values = [avro_body(ORDER_SCHEMA, v) for v in ORDERS]
        headers = [guid_headers(registered.guid)] * 2
    loaded = []
    for backend in BACKENDS:
        # a tombstone has no schema and loads its metadata
        partition = record_partition(
            TOPIC, values + [None], headers=headers and headers + [None]
        )
        pipeline = dlt.pipeline(
            f"avro_{uuid4().hex[:8]}", destination="duckdb", dev_mode=True
        )
        run_processor(
            pipeline,
            TOPIC,
            use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
            AvroProcessor(backend=backend),
            registry_credentials,
        )
        loaded.append(
            (
                pipeline.default_schema.data_table_names(),
                column_types(pipeline, TABLE),
                table_rows(
                    pipeline, TABLE, VALUE_COLUMNS + ["_kafka__key", "_kafka__offset"]
                ),
            )
        )
    python, pyarrow = loaded
    tables, types, rows = python
    assert tables == [TABLE]
    assert {name: types[name] for name in VALUE_COLUMNS} == {
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
    }
    first = dict(zip(VALUE_COLUMNS, rows[0]))
    assert first["amount"] == Decimal("12.34")
    assert first["created"] == datetime(2026, 1, 1, 8, tzinfo=timezone.utc)
    # local timestamps have no time zone
    assert first["local"].replace(tzinfo=None) == datetime(2026, 1, 1, 9, 30)
    # nanoseconds load as microseconds
    assert first["nanos"] == datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)
    assert first["updated"] == datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc)
    assert (first["day"], first["at"]) == (date(2026, 1, 1), time(10, 11, 12))
    assert (first["order_id"], first["status"], bytes(first["hash"])) == (
        "00000000-0000-0000-0000-000000000001",
        "NEW",
        b"\x00\xff",
    )
    assert (first["billing__city"], first["billing__zip"]) == ("Berlin", 10115)
    assert [json.loads(first[name]) for name in ("tags", "attributes", "score")] == [
        ["a", "b"],
        {"weight": 3},
        5,
    ]
    assert first["price"] == "12.34"
    second = dict(zip(VALUE_COLUMNS, rows[1]))
    assert (second["note"], second["billing__city"], json.loads(second["score"])) == (
        None,
        None,
        "high",
    )
    assert rows[2][:-2] == (None,) * len(VALUE_COLUMNS)
    assert rows[2][-2:] == ("key_2", 2)
    assert pyarrow == python


@pytest.mark.parametrize("on_decode_error", ["raise", "skip"])
@pytest.mark.parametrize("backend", BACKENDS)
def test_decode_errors(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    kafka_warnings: List[str],
    backend: Any,
    on_decode_error: Any,
) -> None:
    registry, registry_credentials = mock_registry
    schema = {"type": "record", "name": "R", "fields": [{"name": "a", "type": "long"}]}
    longer = {
        "type": "record",
        "name": "R",
        "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}],
    }
    schema_id = register_avro(registry, f"{TOPIC}-value", schema).schema_id
    json_id = register(registry, "other", {"type": "object"}, "JSON").schema_id
    values = [
        avro_framed(schema_id, schema, {"a": 0}),
        # written with another schema: bytes are left after the value
        avro_framed(schema_id, longer, {"a": 1, "b": "x"}),
        avro_framed(schema_id, schema, {"a": 2})[:-1],
        avro_framed(99, schema, {"a": 3}),
        avro_body(schema, {"a": 4}) + b"plain",
        framed(json_id, {"a": 5}),
        avro_framed(schema_id, schema, {"a": 6}),
    ]
    partition = record_partition(TOPIC, values)
    credentials = use_fake_consumer(FakeConsumer({TOPIC: [partition]}))
    processor = AvroProcessor(backend=backend, on_decode_error=on_decode_error)
    if on_decode_error == "raise":
        with pytest.raises(PipelineStepFailed) as py_ex:
            run_processor(pipeline, TOPIC, credentials, processor, registry_credentials)
        error = next(
            e for e in exception_chain(py_ex.value) if isinstance(e, MessageDecodeError)
        )
        assert error.offset == 1
        assert "bytes are left after the value" in str(error)
        return
    run_processor(pipeline, TOPIC, credentials, processor, registry_credentials)
    assert loaded_offsets(pipeline, TABLE) == [(0, 0), (0, 6)]
    assert skipped_offsets(kafka_warnings) == [1, 2, 3, 4, 5]
    skipped = [w for w in kafka_warnings if w.startswith("Skipped")]
    assert "not in the Schema Registry" in skipped[2]
    assert "no schema id" in skipped[3]
    assert "a JSON schema" in skipped[4]


@pytest.mark.parametrize("backend", BACKENDS)
def test_keys(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    backend: Any,
) -> None:
    registry, registry_credentials = mock_registry
    schema = {"type": "record", "name": "R", "fields": [{"name": "a", "type": "long"}]}
    key_schema = {
        "type": "record",
        "name": "Key",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "region", "type": "string"},
        ],
    }
    value_id = register_avro(registry, f"{TOPIC}-value", schema).schema_id
    key_id = register_avro(registry, f"{TOPIC}-key", key_schema).schema_id
    values = [avro_framed(value_id, schema, {"a": a}) for a in range(4)] + [None]
    keys = [
        avro_framed(key_id, key_schema, {"id": 1, "region": "eu"}),
        # a long key that starts with a 0 byte and makes schema id 0
        (65).to_bytes(8, "big"),
        b"plain",
        None,
        # a tombstone keeps its key
        avro_framed(key_id, key_schema, {"id": 1, "region": "eu"}),
    ]
    partition = record_partition(TOPIC, values, keys=keys)
    run_processor(
        pipeline,
        TOPIC,
        use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
        AvroProcessor(backend=backend),
        registry_credentials,
    )
    types = column_types(pipeline, TABLE)
    assert (types["_kafka__key__id"], types["_kafka__key__region"]) == (
        "bigint",
        "text",
    )
    rows = table_rows(
        pipeline,
        TABLE,
        ["a", "_kafka__key__id", "_kafka__key__region", "_kafka__key", "_kafka_msg_id"],
    )
    # the long key loads as text, inserts into duckdb drop its 0 characters, parquet keeps them
    assert [row[:3] + (row[3] and row[3].lstrip("\x00"),) for row in rows] == [
        (0, 1, "eu", None),
        (1, None, None, "A"),
        (2, None, None, "plain"),
        (3, None, None, None),
        (None, 1, "eu", None),
    ]
    # messages of a key share the id
    assert rows[0][4] == rows[4][4]


def test_scalar_key_and_value(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
) -> None:
    registry, registry_credentials = mock_registry
    value_id = register_avro(registry, f"{TOPIC}-value", "string").schema_id
    key_id = register_avro(registry, f"{TOPIC}-key", "long").schema_id
    partition = record_partition(
        TOPIC,
        [avro_framed(value_id, "string", "hello")],
        keys=[avro_framed(key_id, "long", 7)],
    )
    run_processor(
        pipeline,
        TOPIC,
        use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
        AvroProcessor(),
        registry_credentials,
    )
    assert table_rows(pipeline, TABLE, ["value", "_kafka__key"]) == [("hello", 7)]


@pytest.mark.parametrize("table_per_schema", [False, True])
@pytest.mark.parametrize("backend", BACKENDS)
def test_union_of_records(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    backend: Any,
    table_per_schema: bool,
) -> None:
    registry, registry_credentials = mock_registry
    created = {
        "type": "record",
        "name": "Created",
        "namespace": "com.shop",
        "fields": [{"name": "id", "type": "long"}],
    }
    shipped = {
        "type": "record",
        "name": "Shipped",
        "namespace": "com.shop",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "shipping", "type": ["null", ADDRESS]},
        ],
    }
    # one topic with several event types, as the Confluent docs describe it
    union = [created, shipped, "com.shop.Address"]
    schema_id = register_avro(registry, f"{TOPIC}-value", union).schema_id
    events = [
        ("com.shop.Created", {"id": 1}),
        ("com.shop.Shipped", {"id": 1, "shipping": {"city": "Berlin", "zip": 10115}}),
        ("com.shop.Created", {"id": 2}),
    ]
    values = [avro_framed(schema_id, union, event) for event in events]
    run_processor(
        pipeline,
        TOPIC,
        use_fake_consumer(FakeConsumer({TOPIC: [record_partition(TOPIC, values)]})),
        AvroProcessor(
            backend=backend,
            table_per_schema=table_per_schema,
            table_name=lambda topic, name: name.rsplit(".", 1)[1].lower(),
        ),
        registry_credentials,
    )
    if table_per_schema:
        assert sorted(pipeline.default_schema.data_table_names()) == [
            "created",
            "shipped",
        ]
        assert table_rows(pipeline, "created", ["id", "_kafka__offset"]) == [
            (1, 0),
            (2, 2),
        ]
        assert table_rows(
            pipeline, "shipped", ["id", "shipping__city", "_kafka__offset"]
        ) == [(1, "Berlin", 1)]
    else:
        assert table_rows(
            pipeline, TABLE, ["id", "shipping__city", "_kafka__offset"]
        ) == [
            (1, None, 0),
            (1, "Berlin", 1),
            (2, None, 2),
        ]


@pytest.mark.parametrize("backend", BACKENDS)
def test_schema_references(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    backend: Any,
) -> None:
    from confluent_kafka.schema_registry import SchemaReference

    registry, registry_credentials = mock_registry
    address = register_avro(registry, "address", ADDRESS)
    order = {
        "type": "record",
        "name": "Order",
        "namespace": "com.shop",
        "fields": [
            {"name": "billing", "type": "Address"},
            {"name": "shipping", "type": "Address"},
        ],
    }
    reference = SchemaReference("com.shop.Address", "address", address.version)
    schema_id = register_avro(
        registry, f"{TOPIC}-value", order, references=[reference]
    ).schema_id
    full = {
        **order,
        "fields": [{"name": "billing", "type": ADDRESS}, order["fields"][1]],
    }
    value = {
        "billing": {"city": "Berlin", "zip": 1},
        "shipping": {"city": "Paris", "zip": 2},
    }
    partition = record_partition(TOPIC, [avro_framed(schema_id, full, value)])
    run_processor(
        pipeline,
        TOPIC,
        use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
        AvroProcessor(backend=backend),
        registry_credentials,
    )
    assert table_rows(pipeline, TABLE, ["billing__city", "shipping__city"]) == [
        ("Berlin", "Paris")
    ]


def test_hints_before_first_message(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
) -> None:
    registry, registry_credentials = mock_registry
    register_avro(registry, f"{TOPIC}-value", ORDER_SCHEMA)
    register_avro(registry, f"{TOPIC}-key", "long")
    # only a tombstone without a key: the columns come from the latest schemas
    partition = record_partition(TOPIC, [None], keys=[None])
    resource = kafka_consumer(
        TOPIC,
        credentials=use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
        msg_processor=AvroProcessor(),
        schema_registry=registry_credentials,
    )
    resource.apply_hints(columns={"id": {"data_type": "text"}})
    pipeline.run(resource)
    types = column_types(pipeline, TABLE)
    assert (types["amount"], types["_kafka__key"], types["id"]) == (
        "decimal",
        "bigint",
        "text",
    )


def test_data_contract_rules_warn(
    pipeline: dlt.Pipeline,
    use_fake_consumer: UseFakeConsumer,
    mock_registry: MockRegistry,
    kafka_warnings: List[str],
) -> None:
    from confluent_kafka.schema_registry.schema_registry_client import RuleSet

    registry, registry_credentials = mock_registry
    schema = {
        "type": "record",
        "name": "R",
        "fields": [{"name": "a", "type": "string"}],
    }
    rules = RuleSet.from_dict(
        {
            "domainRules": [
                {
                    "name": "pii",
                    "kind": "TRANSFORM",
                    "mode": "WRITEREAD",
                    "type": "ENCRYPT",
                }
            ]
        }
    )
    schema_id = register_avro(
        registry, f"{TOPIC}-value", schema, rule_set=rules
    ).schema_id
    values = [avro_framed(schema_id, schema, {"a": "x"})] * 2
    run_processor(
        pipeline,
        TOPIC,
        use_fake_consumer(FakeConsumer({TOPIC: [record_partition(TOPIC, values)]})),
        AvroProcessor(),
        registry_credentials,
    )
    assert len([w for w in kafka_warnings if "data contract rules" in w]) == 1


def test_requires_registry(
    pipeline: dlt.Pipeline, use_fake_consumer: UseFakeConsumer
) -> None:
    partition = record_partition(TOPIC, [b"\x00\x00\x00\x00\x01\x02"])
    with pytest.raises(PipelineStepFailed) as py_ex:
        run_processor(
            pipeline,
            TOPIC,
            use_fake_consumer(FakeConsumer({TOPIC: [partition]})),
            AvroProcessor(),
            None,
        )
    assert "needs a Schema Registry" in str(py_ex.value)


def test_without_fastavro(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "fastavro", None)
    with pytest.raises(MissingDependencyException):
        AvroProcessor()
