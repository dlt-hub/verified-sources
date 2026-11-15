from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

from dlt.common import json
from dlt.common.schema.typing import TColumnSchema, TDataType

from .columns import TConversion, TFields, TPath, TypedField
from .registry import TSchemaId

JSON_STRING_FORMATS: Dict[str, TDataType] = {
    "date-time": "timestamp",
    "date": "date",
    "time": "time",
}


def json_schema_fields(schema: Mapping[str, Any]) -> TFields:
    """Columns of the objects valid against a JSON Schema.

    Nested objects with `properties` are flattened into nested columns, other objects and
    arrays are `json`. Local `$ref` are resolved. `required` does not make columns NOT NULL:
    messages written with older schemas can lack them.
    """
    fields: TFields = {}
    root = _resolve(schema, schema)
    for name, prop in root.get("properties", {}).items():
        _add_fields(fields, (name,), prop, schema)
    return fields


def top_level_fields(fields: TFields) -> Set[str]:
    """Top-level JSON properties of the flattened `fields`"""
    return {path[0] for path in fields}


def json_schema_name(registry: Any, schema_id: TSchemaId, schema: Any) -> str:
    """Name of a registered JSON Schema: its `title`, which JSON serializers use as the record
    name, else its subject"""
    title = json.loads(schema.schema_str).get("title")
    if title:
        return str(title)
    kind, value = schema_id
    if kind == "id":
        subjects = registry.get_subjects_by_schema_id(value)
        if subjects:
            return str(subjects[0])
    return str(value)


def _add_fields(
    fields: TFields, path: TPath, prop: Any, root: Mapping[str, Any]
) -> None:
    prop = _resolve(prop, root)
    if not isinstance(prop, Mapping):
        return
    branches = _branches(prop, root)
    if len(branches) == 1 and "properties" in branches[0]:
        for name, child in branches[0]["properties"].items():
            _add_fields(fields, path + (name,), child, root)
        return
    data_type = _data_type(branches)
    if data_type is not None:
        column: TColumnSchema = {"data_type": data_type}
        conversion: Optional[TConversion] = None
        if data_type == "timestamp":
            column["timezone"] = True
            conversion = ("iso_timestamp", None)
        fields[path] = TypedField(column, conversion)


def _branches(
    prop: Mapping[str, Any], root: Mapping[str, Any]
) -> List[Mapping[str, Any]]:
    """Alternatives of `prop` without `null`, one per type"""
    for key in ("oneOf", "anyOf"):
        if key in prop:
            branches: List[Mapping[str, Any]] = []
            for branch in prop[key]:
                branches.extend(_branches(_resolve(branch, root), root))
            return branches
    types = prop.get("type")
    if isinstance(types, list):
        return [{**prop, "type": t} for t in types if t != "null"]
    if types == "null":
        return []
    if types is None and "properties" not in prop:
        return []
    return [prop]


def _data_type(branches: Sequence[Mapping[str, Any]]) -> Optional[TDataType]:
    types = {branch.get("type", "object") for branch in branches}
    if not types:
        return None
    if types == {"integer", "number"}:
        return "double"
    if len(branches) > 1:
        return "json"
    branch = branches[0]
    json_type = branch.get("type", "object")
    if json_type == "integer":
        return "bigint"
    if json_type == "number":
        return "double"
    if json_type == "boolean":
        return "bool"
    if json_type == "string":
        return JSON_STRING_FORMATS.get(branch.get("format", ""), "text")
    return "json"


def _resolve(prop: Any, root: Mapping[str, Any]) -> Any:
    """Follows local `$ref`, other references stay unresolved and load as `json`"""
    seen = set()
    while isinstance(prop, Mapping) and isinstance(prop.get("$ref"), str):
        ref = prop["$ref"]
        if not ref.startswith("#/") or ref in seen:
            return {"type": "object"}
        seen.add(ref)
        target: Any = root
        for part in ref[2:].split("/"):
            part = part.replace("~1", "/").replace("~0", "~")
            if not isinstance(target, Mapping) or part not in target:
                return {"type": "object"}
            target = target[part]
        prop = target
    return prop
