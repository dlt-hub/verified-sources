"""Maps Jira field metadata to dlt data types and columns, converts field values to them"""

from typing import Any, Callable, Dict, Iterable, Optional

from dlt.common.data_types.type_helpers import coerce_value, py_type_to_sc_type
from dlt.common.json import json
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.time import ensure_date, ensure_datetime_in_tz
from dlt.common.typing import DictStrAny, TDataItem

from ..settings import URL_KEYS

# types of values with a known structure, which the normalizer unnests
OBJECT = "object"
ARRAY = "array"

# by `schema.type` of a field and `schema.items` of an array field
SCHEMA_TYPES: Dict[str, str] = {
    "string": "text",
    "number": "double",
    "datetime": "timestamp",
    "date": "date",
    **dict.fromkeys(
        (
            "attachment",
            "component",
            "group",
            "issuelinks",
            "issuetype",
            "option",
            "option-with-child",
            "priority",
            "progress",
            "project",
            "resolution",
            "securitylevel",
            "status",
            "statusCategory",
            "timetracking",
            "user",
            "version",
            "votes",
            "watches",
        ),
        OBJECT,
    ),
}
# system fields of a type their schema does not tell
SYSTEM_FIELD_TYPES: Dict[str, str] = {
    # Jira returns `parent` without schema
    "parent": OBJECT,
    # durations in seconds
    "timespent": "bigint",
    "timeestimate": "bigint",
    "timeoriginalestimate": "bigint",
    "aggregatetimespent": "bigint",
    "aggregatetimeestimate": "bigint",
    "aggregatetimeoriginalestimate": "bigint",
    "workratio": "bigint",
}
# custom fields of a type their schema does not tell, by `schema.custom`
CUSTOM_FIELD_TYPES: Dict[str, str] = {
    "com.pyxis.greenhopper.jira:gh-sprint": ARRAY,
    "com.pyxis.greenhopper.jira:gh-lexo-rank": "text",
}
ID_COLUMNS: TTableSchemaColumns = {
    "id": {"name": "id", "data_type": "text", "nullable": False},
    "key": {"name": "key", "data_type": "text"},
}


def get_field_type(field: Dict[str, Any]) -> Optional[str]:
    """Returns the dlt data type of a field from `/field` metadata, `object` or `array` for
    values the normalizer unnests.

    Returns None for fields with values of unknown structure, e.g. fields without schema, of type
    `any` or added by apps. Those are loaded as text, objects and arrays serialized as json.
    """
    schema = field.get("schema") or {}
    if not field.get("custom") and field["id"] in SYSTEM_FIELD_TYPES:
        return SYSTEM_FIELD_TYPES[field["id"]]
    if schema.get("custom") in CUSTOM_FIELD_TYPES:
        return CUSTOM_FIELD_TYPES[schema["custom"]]
    if schema.get("type") == "array":
        return ARRAY if schema.get("items") in SCHEMA_TYPES else None
    return SCHEMA_TYPES.get(schema.get("type"))


def field_to_column_schema(field: Dict[str, Any]) -> Optional[TColumnSchema]:
    """Converts a field from `/field` metadata into a dlt column schema.

    Returns None for objects and arrays, values of unknown structure are text.
    """
    field_type = get_field_type(field)
    if field_type is None:
        return {"name": field["id"], "data_type": "text"}
    if field_type in (OBJECT, ARRAY):
        return None
    return {"name": field["id"], "data_type": field_type}  # type: ignore[typeddict-item]


def fields_to_columns(fields: Iterable[Dict[str, Any]]) -> TTableSchemaColumns:
    """Converts `fields` into dlt columns that follow the issue `id` and `key` columns"""
    columns: TTableSchemaColumns = {name: dict(col) for name, col in ID_COLUMNS.items()}  # type: ignore[misc]
    for field in fields:
        col = field_to_column_schema(field)
        if col is not None:
            columns[col["name"]] = col
    return columns


def coerce_scalar(value: Any, data_type: str) -> Any:
    """Converts a scalar `value` to the python type of `data_type`, e.g. `3` to `3.0` for a
    `double`. Raises ValueError if it cannot be converted, e.g. `3.5` to `bigint`.
    """
    if data_type in ("timestamp", "date"):
        # numbers are not dates, `ensure_datetime_in_tz` would read them as UNIX timestamps
        if isinstance(value, (int, float)):
            raise ValueError(value)
        return (
            ensure_date(value) if data_type == "date" else ensure_datetime_in_tz(value)
        )
    return coerce_value(data_type, py_type_to_sc_type(type(value)), value)  # type: ignore[arg-type]


def drop_keys(keys: Iterable[str] = URL_KEYS) -> Callable[[TDataItem], TDataItem]:
    """Returns a function for `add_map` that removes `keys` from rows and their nested values in
    place, by default REST links and avatar and icon urls, e.g.
    `jira_issues().add_map(drop_keys())`
    """
    dropped = set(keys)

    def _drop(item: TDataItem) -> TDataItem:
        if isinstance(item, dict):
            for key in dropped.intersection(item):
                del item[key]
            values: Iterable[Any] = item.values()
        else:
            values = item
        for value in values:
            if isinstance(value, (dict, list)):
                _drop(value)
        return item

    return _drop


def convert_value(value: Any, field_type: Optional[str]) -> Any:
    """Converts the value of a field of `field_type`: scalars to the python type of their dlt data
    type, objects and arrays are returned as is and values of unknown structure as text.

    Raises ValueError when `value` is of a different kind, e.g. a string where an object is
    expected, or when a scalar cannot be converted.
    """
    if value is None:
        return None
    if field_type is None:
        if isinstance(value, (dict, list)) and not value:
            return None
        return value if isinstance(value, str) else json.dumps(value)
    if field_type in (OBJECT, ARRAY):
        if not isinstance(value, dict if field_type == OBJECT else list):
            raise ValueError(value)
        return value
    if isinstance(value, (dict, list)):
        raise ValueError(value)
    return coerce_scalar(value, field_type)


def convert_record(record: DictStrAny, types: Dict[str, str]) -> DictStrAny:
    """Converts the values of `types` keys of `record` in place to their dlt data types"""
    for key, data_type in types.items():
        if record.get(key) is not None:
            record[key] = coerce_scalar(record[key], data_type)
    return record
