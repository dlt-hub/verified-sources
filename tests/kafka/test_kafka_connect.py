from typing import Any, Dict, Optional

import pytest

from sources.kafka.helpers.kafka_connect import (
    connect_fields,
    connect_payload,
    connect_schema,
)


@pytest.mark.parametrize(
    "field,expected,conversion",
    [
        ({"type": "int16"}, {"data_type": "bigint"}, None),
        ({"type": "float32"}, {"data_type": "double"}, None),
        ({"type": "bytes"}, {"data_type": "binary"}, ("base64", None)),
        (
            {
                "type": "bytes",
                "name": "org.apache.kafka.connect.data.Decimal",
                "parameters": {"scale": "4"},
            },
            {"data_type": "decimal", "precision": 38, "scale": 4},
            ("decimal", 4),
        ),
        (
            {"type": "int64", "name": "org.apache.kafka.connect.data.Timestamp"},
            {"data_type": "timestamp", "timezone": True},
            ("epoch", "ms"),
        ),
        (
            {"type": "int64", "name": "io.debezium.time.NanoTimestamp"},
            {"data_type": "timestamp", "timezone": True},
            ("epoch", "ns"),
        ),
        (
            {"type": "int32", "name": "org.apache.kafka.connect.data.Date"},
            {"data_type": "date"},
            ("days", None),
        ),
        (
            {"type": "int32", "name": "org.apache.kafka.connect.data.Time"},
            {"data_type": "time"},
            ("time", "ms"),
        ),
        (
            {"type": "string", "name": "io.debezium.data.Json"},
            {"data_type": "json"},
            ("json", None),
        ),
        ({"type": "map", "keys": {"type": "string"}}, {"data_type": "json"}, None),
    ],
    ids=[
        "int16",
        "float32",
        "bytes",
        "decimal",
        "timestamp",
        "nano_timestamp",
        "date",
        "time",
        "debezium_json",
        "map",
    ],
)
def test_connect_field(
    field: Dict[str, Any], expected: Dict[str, Any], conversion: Optional[Any]
) -> None:
    schema = {"type": "struct", "fields": [{**field, "field": "f"}]}
    field = connect_fields(schema)[("f",)]
    assert field.column == expected
    assert field.conversion == conversion


def test_connect_nested_struct() -> None:
    schema = {
        "type": "struct",
        "fields": [
            {
                "type": "struct",
                "field": "after",
                "fields": [
                    {"type": "int32", "field": "id"},
                    {"type": "string", "field": "name"},
                ],
            },
            {"type": "string", "field": "op"},
        ],
    }
    assert list(connect_fields(schema)) == [("after", "id"), ("after", "name"), ("op",)]


def test_connect_envelope_parts() -> None:
    value = b'{"schema":{"type":"struct","fields":[]},"payload":{"a":{"payload":1}}}'
    assert connect_schema(value) == b'{"type":"struct","fields":[]}'
    assert connect_payload(value) == b'{"a":{"payload":1}}'
    assert connect_payload(b'{"a":1}') is None
    assert connect_schema(b'{"a":1}') is None
