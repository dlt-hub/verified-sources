from typing import Any, Dict, List, Optional, Tuple

import fastavro
import pytest
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)

from sources.kafka.helpers.avro import (
    avro_fields,
    decode_schema,
    is_record,
    record_name,
    union_fields,
)
from sources.kafka.helpers.columns import fields_columns

DECIMAL_10_2 = {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}


def field_id(field_type: Any) -> str:
    """Test id of an Avro field type: its type, logical type or Kafka Connect name"""
    if isinstance(field_type, str):
        return field_type
    if isinstance(field_type, list):
        return "_".join(field_id(branch) for branch in field_type)
    name = (
        field_type.get("logicalType")
        or field_type.get("connect.name", "").rsplit(".")[-1]
    )
    return f"{field_type['type']}_{name}" if name else field_type["type"]


def parse(schema: Any, *references: Any) -> Tuple[Any, Dict[str, Any]]:
    """`schema` parsed by fastavro after its references, and its named types"""
    named: Dict[str, Any] = {}
    for reference in references:
        fastavro.parse_schema(decode_schema(reference), named)
    return fastavro.parse_schema(decode_schema(schema), named), named


FIELD_CASES: List[Tuple[Any, Dict[str, Any], Any]] = [
    ("boolean", {"data_type": "bool"}, None),
    ("int", {"data_type": "bigint"}, None),
    ("long", {"data_type": "bigint"}, None),
    ("float", {"data_type": "double"}, None),
    ("string", {"data_type": "text"}, None),
    ("bytes", {"data_type": "binary"}, None),
    ({"type": "enum", "name": "E", "symbols": ["A"]}, {"data_type": "text"}, None),
    ({"type": "fixed", "name": "F", "size": 4}, {"data_type": "binary"}, None),
    ({"type": "array", "items": "long"}, {"data_type": "json"}, None),
    ({"type": "map", "values": "long"}, {"data_type": "json"}, None),
    (["null", "string"], {"data_type": "text"}, None),
    (["string", "null"], {"data_type": "text"}, None),
    (["string"], {"data_type": "text"}, None),
    (["int", "long"], {"data_type": "bigint"}, None),
    (["null", "int", "double"], {"data_type": "double"}, None),
    (["null", "long", "string"], {"data_type": "json"}, None),
    (
        DECIMAL_10_2,
        {"data_type": "decimal", "precision": 10, "scale": 2},
        None,
    ),
    (
        {"type": "bytes", "logicalType": "decimal", "precision": 10},
        {"data_type": "decimal", "precision": 10, "scale": 0},
        None,
    ),
    (
        {
            "type": "fixed",
            "name": "D",
            "size": 8,
            "logicalType": "decimal",
            "precision": 18,
            "scale": 3,
        },
        {"data_type": "decimal", "precision": 18, "scale": 3},
        None,
    ),
    (
        {"type": "bytes", "logicalType": "decimal", "precision": 64, "scale": 2},
        {"data_type": "text"},
        ("str", None),
    ),
    (
        {"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 5},
        {"data_type": "binary"},
        None,
    ),
    (
        {
            "type": "fixed",
            "name": "D",
            "size": 4,
            "logicalType": "decimal",
            "precision": 20,
        },
        {"data_type": "binary"},
        None,
    ),
    (
        {"type": "bytes", "logicalType": "big-decimal"},
        {"data_type": "binary"},
        None,
    ),
    (
        {"type": "string", "logicalType": "uuid"},
        {"data_type": "text"},
        ("uuid", None),
    ),
    (
        {"type": "fixed", "name": "U", "size": 16, "logicalType": "uuid"},
        {"data_type": "text"},
        ("uuid", None),
    ),
    ({"type": "int", "logicalType": "date"}, {"data_type": "date"}, None),
    ({"type": "int", "logicalType": "time-millis"}, {"data_type": "time"}, None),
    ({"type": "long", "logicalType": "time-micros"}, {"data_type": "time"}, None),
    (
        {"type": "long", "logicalType": "timestamp-millis"},
        {"data_type": "timestamp", "timezone": True},
        None,
    ),
    (
        {"type": "long", "logicalType": "timestamp-nanos"},
        {"data_type": "timestamp", "timezone": True},
        ("epoch", "ns"),
    ),
    (
        {"type": "long", "logicalType": "local-timestamp-micros"},
        {"data_type": "timestamp", "timezone": False},
        None,
    ),
    (
        {"type": "long", "logicalType": "local-timestamp-nanos"},
        {"data_type": "timestamp", "timezone": False},
        ("local_epoch", "ns"),
    ),
    (
        {"type": "fixed", "name": "Du", "size": 12, "logicalType": "duration"},
        {"data_type": "binary"},
        None,
    ),
    (
        {"type": "string", "logicalType": "my-type"},
        {"data_type": "text"},
        None,
    ),
    (
        {"type": "int", "logicalType": "timestamp-millis"},
        {"data_type": "bigint"},
        None,
    ),
    (
        {"type": "long", "connect.name": "io.debezium.time.MicroTimestamp"},
        {"data_type": "timestamp", "timezone": True},
        ("epoch", "us"),
    ),
    (
        {"type": "int", "connect.name": "io.debezium.time.Date"},
        {"data_type": "date"},
        ("days", None),
    ),
    (
        {"type": "long", "connect.name": "io.debezium.time.MicroTime"},
        {"data_type": "time"},
        ("time", "us"),
    ),
    (
        {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"},
        {"data_type": "timestamp", "timezone": True},
        ("iso_timestamp", None),
    ),
    (
        {"type": "string", "connect.name": "io.debezium.data.Json"},
        {"data_type": "json"},
        ("json", None),
    ),
    (
        {
            "type": "record",
            "name": "VariableScaleDecimal",
            "namespace": "io.debezium.data",
            "fields": [
                {"name": "scale", "type": "int"},
                {"name": "value", "type": "bytes"},
            ],
        },
        {"data_type": "text"},
        ("variable_decimal", None),
    ),
    # Kafka Connect writes both the logical type and its name, fastavro converts it
    (
        {
            "type": "long",
            "logicalType": "timestamp-millis",
            "connect.name": "org.apache.kafka.connect.data.Timestamp",
        },
        {"data_type": "timestamp", "timezone": True},
        None,
    ),
]


@pytest.mark.parametrize(
    "field_type,expected,conversion",
    [pytest.param(*case, id=field_id(case[0])) for case in FIELD_CASES],
)
def test_field_columns(
    field_type: Any, expected: Dict[str, Any], conversion: Optional[Any]
) -> None:
    schema = {
        "type": "record",
        "name": "R",
        "fields": [{"name": "f", "type": field_type}],
    }
    field = avro_fields(*parse(schema))[("f",)]
    assert field.column == expected
    assert field.conversion == conversion


@pytest.mark.parametrize(
    "field_type",
    [
        "null",
        ["null"],
        {"type": "record", "name": "Empty", "fields": []},
    ],
    ids=["null", "null_union", "empty_record"],
)
def test_fields_without_columns(field_type: Any) -> None:
    schema = {
        "type": "record",
        "name": "R",
        "fields": [{"name": "f", "type": field_type}],
    }
    assert avro_fields(*parse(schema)) == {}


def test_nested_and_named_records() -> None:
    schema = {
        "type": "record",
        "name": "Order",
        "namespace": "com.shop",
        "fields": [
            {
                "name": "billing",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Address",
                        "fields": [
                            {"name": "city", "type": "string"},
                            # a recursive type can't be flattened
                            {"name": "previous", "type": ["null", "Address"]},
                        ],
                    },
                ],
            },
            {"name": "shipping", "type": "com.shop.Address"},
            {"name": "customer", "type": "Customer"},
        ],
    }
    # a type defined in a referenced schema
    customer = {
        "type": "record",
        "name": "Customer",
        "namespace": "com.shop",
        "fields": [{"name": "id", "type": "long"}],
    }
    columns = fields_columns(
        avro_fields(*parse(schema, customer)), SnakeCaseNamingConvention()
    )
    assert {name: column["data_type"] for name, column in columns.items()} == {
        "billing__city": "text",
        "billing__previous": "json",
        "shipping__city": "text",
        "shipping__previous": "json",
        "customer__id": "bigint",
    }
    with pytest.raises(Exception, match="Customer"):
        parse(schema)


def test_scalar_and_key_schemas() -> None:
    assert list(avro_fields(*parse("string"))) == [("value",)]
    map_field = avro_fields(*parse({"type": "map", "values": "long"}))[("value",)]
    assert map_field.column == {"data_type": "json"}
    key = {"type": "record", "name": "Key", "fields": [{"name": "id", "type": "long"}]}
    key_fields = avro_fields(*parse(key), prefix=("_kafka", "key"), scalar_name=None)
    assert list(key_fields) == [("_kafka", "key", "id")]
    scalar_key = avro_fields(*parse("long"), prefix=("_kafka", "key"), scalar_name=None)
    assert list(scalar_key) == [("_kafka", "key")]
    assert is_record(*parse(key)) and not is_record(*parse("string"))
    variable_scale_decimal = {
        "type": "record",
        "name": "VariableScaleDecimal",
        "namespace": "io.debezium.data",
        "fields": [],
    }
    assert not is_record(*parse(variable_scale_decimal))


def test_top_level_union() -> None:
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
            {"name": "carrier", "type": "string"},
        ],
    }
    cancelled = {
        "type": "record",
        "name": "Cancelled",
        "namespace": "com.shop",
        "fields": [{"name": "id", "type": "string"}],
    }
    union, named = parse([created, shipped, cancelled])
    fields = union_fields(union, named)
    assert list(fields) == [("id",), ("carrier",)]
    # a field with other types in other branches loads as json
    assert fields[("id",)].column == {"data_type": "json"}
    assert record_name(union[1], named) == "com.shop.Shipped"
    assert record_name("string", named) == "string"
    connect_default = {
        "type": "record",
        "name": "io.confluent.connect.avro.ConnectDefault",
        "fields": [],
    }
    assert record_name(*parse(connect_default)) is None


def test_decode_schema() -> None:
    schema = {
        "type": "record",
        "name": "R",
        "fields": [
            {
                "name": "bad",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 2,
                    "scale": 5,
                },
            },
            # writers may register defaults fastavro rejects, values never use them
            {"name": "count", "type": ["null", "long"], "default": "zero"},
            {"name": "amount", "type": DECIMAL_10_2},
        ],
    }
    decoded = decode_schema(schema)
    assert decoded["fields"][0]["type"] == {"type": "bytes", "precision": 2, "scale": 5}
    assert "default" not in decoded["fields"][1]
    assert decoded["fields"][2]["type"] == DECIMAL_10_2
    with pytest.raises(Exception):
        fastavro.parse_schema(schema)
    fastavro.parse_schema(decoded)
