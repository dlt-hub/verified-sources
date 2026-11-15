import sys
from functools import partial
from typing import Any, List

import dlt
import pytest
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

ORDER_SCHEMA = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "long"},
        {
            "name": "amount",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 12,
                "scale": 2,
            },
        },
    ],
}


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
