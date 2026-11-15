from typing import Any, Dict

import pytest

from sources.kafka.helpers.json_schema import json_schema_fields

TS: Dict[str, Any] = {"data_type": "timestamp", "timezone": True}


@pytest.mark.parametrize(
    "prop,expected",
    [
        ({"type": "integer"}, {"data_type": "bigint"}),
        ({"type": "number"}, {"data_type": "double"}),
        ({"type": "boolean"}, {"data_type": "bool"}),
        ({"type": "string"}, {"data_type": "text"}),
        ({"type": "string", "format": "date-time"}, TS),
        ({"type": "string", "format": "date"}, {"data_type": "date"}),
        ({"type": "string", "format": "time"}, {"data_type": "time"}),
        ({"type": "string", "format": "uuid"}, {"data_type": "text"}),
        ({"type": ["integer", "null"]}, {"data_type": "bigint"}),
        ({"oneOf": [{"type": "null"}, {"type": "string"}]}, {"data_type": "text"}),
        ({"anyOf": [{"type": "integer"}, {"type": "number"}]}, {"data_type": "double"}),
        ({"type": ["string", "integer"]}, {"data_type": "json"}),
        ({"type": "array", "items": {"type": "string"}}, {"data_type": "json"}),
        ({"type": "object"}, {"data_type": "json"}),
        ({"$ref": "#/$defs/when"}, TS),
        ({"$ref": "#/definitions/legacy"}, {"data_type": "bigint"}),
        ({"$ref": "other.json#/x"}, {"data_type": "json"}),
        ({}, None),
        ({"type": "null"}, None),
    ],
    ids=[
        "integer",
        "number",
        "boolean",
        "string",
        "date_time",
        "date",
        "time",
        "uuid",
        "nullable_type_list",
        "nullable_one_of",
        "integer_or_number",
        "union",
        "array",
        "object_without_properties",
        "ref_defs",
        "ref_definitions",
        "ref_other_schema",
        "any",
        "null",
    ],
)
def test_json_schema_field(prop: Dict[str, Any], expected: Any) -> None:
    schema = {
        "type": "object",
        "properties": {"f": prop},
        "required": ["f"],
        "$defs": {"when": {"type": "string", "format": "date-time"}},
        "definitions": {"legacy": {"type": "integer"}},
    }
    fields = json_schema_fields(schema)
    if expected is None:
        assert fields == {}
    else:
        # required does not make columns NOT NULL
        assert {path: field.column for path, field in fields.items()} == {
            ("f",): expected
        }


def test_json_schema_nested_objects() -> None:
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "customer": {
                "type": ["object", "null"],
                "properties": {
                    "name": {"type": "string"},
                    "address": {"$ref": "#/$defs/address"},
                },
            },
        },
        "$defs": {
            "address": {"type": "object", "properties": {"zip": {"type": "string"}}}
        },
    }
    assert {
        path: field.column["data_type"]
        for path, field in json_schema_fields(schema).items()
    } == {
        ("id",): "bigint",
        ("customer", "name"): "text",
        ("customer", "address", "zip"): "text",
    }
