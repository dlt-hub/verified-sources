"""Maps Salesforce `describe()` field types to dlt column schemas"""

from typing import Any, Dict, Iterable, List, Optional

from dlt.common.schema.typing import (
    TColumnSchema,
    TTableSchemaColumns,
    TWriteDisposition,
)

# compound and binary fields cannot be queried with the Bulk API
NON_QUERYABLE_TYPES = {"address", "location", "base64"}
# fields changed on every record update, in order of preference
UPDATED_AT_FIELDS = ("SystemModstamp", "LastModifiedDate")
# fields of insert-only sObjects like field history or login history
CREATED_AT_FIELDS = ("CreatedDate", "LoginTime")
# records may be committed with a timestamp older than the newest one already read
DEFAULT_LAG = 600.0


def sf_field_to_column_schema(
    field: Dict[str, Any], double_as_decimal: bool = False
) -> Optional[TColumnSchema]:
    """Converts a field from sObject `describe()` into a dlt column schema.

    Returns None for fields that the Bulk API cannot query. Unknown types are loaded as text.
    `double` and `percent` fields are loaded as decimals with the field precision and scale
    if `double_as_decimal` is set and the field declares a precision.
    """
    sf_type = field["type"]
    if sf_type in NON_QUERYABLE_TYPES:
        return None
    col: TColumnSchema = {
        "name": field["name"],
        "nullable": field.get("nillable", True),
    }
    if sf_type == "boolean":
        col["data_type"] = "bool"
    elif sf_type in ("int", "long"):
        col["data_type"] = "bigint"
    elif sf_type == "currency" or (
        sf_type in ("double", "percent")
        and double_as_decimal
        and field.get("precision")
    ):
        col["data_type"] = "decimal"
        col["precision"] = field["precision"]
        col["scale"] = field["scale"]
    elif sf_type in ("double", "percent"):
        col["data_type"] = "double"
    elif sf_type == "date":
        col["data_type"] = "date"
    elif sf_type == "datetime":
        col["data_type"] = "timestamp"
        col["precision"] = 3
        col["timezone"] = True
    elif sf_type == "time":
        col["data_type"] = "time"
        col["precision"] = 3
    elif sf_type == "json":
        col["data_type"] = "json"
    else:
        # id, reference, string, picklist, textarea, email etc. and types added in the future
        col["data_type"] = "text"
    return col


def fields_to_columns(
    fields: Iterable[Dict[str, Any]], double_as_decimal: bool = False
) -> TTableSchemaColumns:
    """Converts queryable `describe()` fields into dlt columns, keeping the field order"""
    return {
        col["name"]: col
        for col in (
            sf_field_to_column_schema(field, double_as_decimal) for field in fields
        )
        if col is not None
    }


def get_primary_key(fields: Iterable[Dict[str, Any]]) -> Optional[List[str]]:
    """Returns the record id field as primary key, None if not queried"""
    return [field["name"] for field in fields if field["type"] == "id"] or None


def get_cursor_field(columns: TTableSchemaColumns) -> Optional[str]:
    """Returns the field that tracks record changes, None if the sObject has none"""
    for name in UPDATED_AT_FIELDS + CREATED_AT_FIELDS:
        if name in columns:
            return name
    return None


def get_write_disposition(cursor_field: Optional[str]) -> TWriteDisposition:
    """Merges updated records, appends records of insert-only sObjects, replaces without cursor"""
    if cursor_field is None:
        return "replace"
    if cursor_field in CREATED_AT_FIELDS:
        return "append"
    return "merge"
