"""Maps Jira field metadata to dlt columns and trims nested field values"""

from typing import Any, Dict, Iterable, List, Optional, Union

from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns

# dlt data type of a scalar or the shape of a nested value: keys kept from an object with their
# shapes, the list wraps the shape of its items
TShape = Union[str, Dict[str, "TShape"], List["TShape"]]

CUSTOM_FIELD_TYPE_PREFIX = "com.atlassian.jira.plugin.system.customfieldtypes:"

ID_NAME: TShape = {"id": "text", "name": "text"}
ISSUE_REF: TShape = {"id": "text", "key": "text"}
USER: TShape = {
    "accountId": "text",
    "displayName": "text",
    "emailAddress": "text",
    "active": "bool",
    "accountType": "text",
    "timeZone": "text",
}
STATUS_CATEGORY: TShape = {"id": "bigint", "key": "text", "name": "text"}
STATUS: TShape = {"id": "text", "name": "text", "statusCategory": STATUS_CATEGORY}
ISSUE_TYPE: TShape = {
    "id": "text",
    "name": "text",
    "subtask": "bool",
    "hierarchyLevel": "bigint",
}
PROJECT: TShape = {
    "id": "text",
    "key": "text",
    "name": "text",
    "projectTypeKey": "text",
}
VERSION: TShape = {
    "id": "text",
    "name": "text",
    "archived": "bool",
    "released": "bool",
    "releaseDate": "text",
}
OPTION: TShape = {"id": "text", "value": "text"}
ISSUE_LINK: TShape = {
    "id": "text",
    "type": {"id": "text", "name": "text", "inward": "text", "outward": "text"},
    "inwardIssue": ISSUE_REF,
    "outwardIssue": ISSUE_REF,
}
ATTACHMENT: TShape = {
    "id": "text",
    "filename": "text",
    "mimeType": "text",
    "size": "bigint",
    "created": "text",
    "author": {"accountId": "text", "displayName": "text"},
}
SPRINT: TShape = {
    "id": "bigint",
    "name": "text",
    "state": "text",
    "boardId": "bigint",
    "goal": "text",
    "startDate": "text",
    "endDate": "text",
    "completeDate": "text",
}
PROGRESS: TShape = {"progress": "bigint", "total": "bigint", "percent": "bigint"}

SYSTEM_FIELD_TYPES: Dict[str, TShape] = {
    "status": STATUS,
    "statusCategory": STATUS_CATEGORY,
    "priority": ID_NAME,
    "issuetype": ISSUE_TYPE,
    "project": PROJECT,
    "resolution": ID_NAME,
    "security": ID_NAME,
    "assignee": USER,
    "reporter": USER,
    "creator": USER,
    "parent": ISSUE_REF,
    "labels": ["text"],
    "components": [ID_NAME],
    "fixVersions": [VERSION],
    "versions": [VERSION],
    "issuelinks": [ISSUE_LINK],
    "subtasks": [ISSUE_REF],
    "attachment": [ATTACHMENT],
    "timetracking": {
        "originalEstimateSeconds": "bigint",
        "remainingEstimateSeconds": "bigint",
        "timeSpentSeconds": "bigint",
    },
    "progress": PROGRESS,
    "aggregateprogress": PROGRESS,
    "votes": {"votes": "bigint", "hasVoted": "bool"},
    "watches": {"watchCount": "bigint", "isWatching": "bool"},
    # durations in seconds
    "timespent": "bigint",
    "timeestimate": "bigint",
    "timeoriginalestimate": "bigint",
    "aggregatetimespent": "bigint",
    "aggregatetimeestimate": "bigint",
    "aggregatetimeoriginalestimate": "bigint",
    "workratio": "bigint",
}
CUSTOM_FIELD_TYPES: Dict[str, TShape] = {
    CUSTOM_FIELD_TYPE_PREFIX + "select": OPTION,
    CUSTOM_FIELD_TYPE_PREFIX + "radiobuttons": OPTION,
    CUSTOM_FIELD_TYPE_PREFIX + "multiselect": [OPTION],
    CUSTOM_FIELD_TYPE_PREFIX + "multicheckboxes": [OPTION],
    CUSTOM_FIELD_TYPE_PREFIX
    + "cascadingselect": {"id": "text", "value": "text", "child": OPTION},
    CUSTOM_FIELD_TYPE_PREFIX + "userpicker": USER,
    CUSTOM_FIELD_TYPE_PREFIX + "multiuserpicker": [USER],
    CUSTOM_FIELD_TYPE_PREFIX + "labels": ["text"],
    CUSTOM_FIELD_TYPE_PREFIX + "float": "double",
    CUSTOM_FIELD_TYPE_PREFIX + "textfield": "text",
    CUSTOM_FIELD_TYPE_PREFIX + "textarea": "text",
    CUSTOM_FIELD_TYPE_PREFIX + "url": "text",
    CUSTOM_FIELD_TYPE_PREFIX + "datepicker": "date",
    CUSTOM_FIELD_TYPE_PREFIX + "datetime": "timestamp",
    "com.pyxis.greenhopper.jira:gh-sprint": [SPRINT],
    "com.pyxis.greenhopper.jira:gh-lexo-rank": "text",
}
SCHEMA_TYPES: Dict[str, TShape] = {
    "string": "text",
    "number": "double",
    "datetime": "timestamp",
    "date": "date",
    "group": {"name": "text", "groupId": "text"},
    "user": USER,
    "option": OPTION,
    "version": VERSION,
    "component": ID_NAME,
    "priority": ID_NAME,
    "resolution": ID_NAME,
    "status": STATUS,
    "issuetype": ISSUE_TYPE,
    "project": PROJECT,
}
ID_COLUMNS: TTableSchemaColumns = {
    "id": {"name": "id", "data_type": "text", "nullable": False},
    "key": {"name": "key", "data_type": "text"},
}


def get_field_type(field: Dict[str, Any]) -> Optional[TShape]:
    """Returns the dlt data type or the shape of a field from `/field` metadata.

    Returns None for fields with values of unknown structure, e.g. fields without schema, of type
    `any` or added by apps. Those are loaded as text, objects and lists serialized as json.
    """
    schema = field.get("schema") or {}
    if not field.get("custom") and field["id"] in SYSTEM_FIELD_TYPES:
        return SYSTEM_FIELD_TYPES[field["id"]]
    if schema.get("custom") in CUSTOM_FIELD_TYPES:
        return CUSTOM_FIELD_TYPES[schema["custom"]]
    if schema.get("type") == "array":
        item_type = SCHEMA_TYPES.get(schema.get("items"))
        return None if item_type is None else [item_type]
    return SCHEMA_TYPES.get(schema.get("type"))


def field_to_column_schema(field: Dict[str, Any]) -> Optional[TColumnSchema]:
    """Converts a field from `/field` metadata into a dlt column schema.

    Returns None for nested values of a known shape: the normalizer flattens their objects and
    loads their lists into nested tables. Values of unknown shape are loaded as text.
    """
    shape = get_field_type(field)
    if shape is None:
        return {"name": field["id"], "data_type": "text"}
    if isinstance(shape, str):
        return {"name": field["id"], "data_type": shape}  # type: ignore[typeddict-item]
    return None


def fields_to_columns(fields: Iterable[Dict[str, Any]]) -> TTableSchemaColumns:
    """Converts `fields` into dlt columns that follow the issue `id` and `key` columns"""
    columns: TTableSchemaColumns = {name: dict(col) for name, col in ID_COLUMNS.items()}  # type: ignore[misc]
    for field in fields:
        col = field_to_column_schema(field)
        if col is not None:
            columns[col["name"]] = col
    return columns


def trim_to_shape(value: Any, shape: Optional[TShape]) -> Any:
    """Keeps the keys of `shape` in nested `value`. Values of unknown shape are returned as is.

    Raises ValueError when `value` is of a different kind than `shape`, e.g. a string where an
    object is expected.
    """
    if value is None or shape is None:
        return value
    if isinstance(shape, dict):
        if not isinstance(value, dict):
            raise ValueError(value)
        return {
            key: trim_to_shape(value[key], shape[key]) for key in shape if key in value
        }
    if isinstance(shape, list):
        if not isinstance(value, list):
            raise ValueError(value)
        return [trim_to_shape(item, shape[0]) for item in value]
    if isinstance(value, (dict, list)):
        raise ValueError(value)
    return value
