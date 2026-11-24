from typing import Any, Dict, TypedDict, Optional, cast

import dlt

from ..typing import TDataPage


class TFieldMapping(TypedDict):
    name: str
    normalized_name: str
    options: Optional[Dict[str, str]]
    field_type: str


def update_fields_mapping(
    new_fields_mapping: TDataPage, existing_fields_mapping: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Specific function to perform data munging and push changes to custom fields' mapping stored in dlt's state
    The endpoint must be an entity fields' endpoint
    """
    for data_item in new_fields_mapping:
        # 'edit_flag' field contains a boolean value, which is set to 'True' for custom fields and 'False' otherwise.
        if data_item.get("edit_flag"):
            # Regarding custom fields, 'key' field contains pipedrive's hash string representation of its name
            # We assume that pipedrive's hash strings are meant to be an univoque representation of custom fields' name, so dlt's state shouldn't be updated while those values
            # remain unchanged
            existing_fields_mapping = _update_field(data_item, existing_fields_mapping)
        # Built in enum and set fields are mapped if their options have int ids
        # Enum fields with bool and string key options are left intact
        elif data_item.get("field_type") in {"set", "enum"}:
            options = data_item.get("options", [])
            first_option = options[0]["id"] if len(options) >= 1 else None
            if isinstance(first_option, int) and not isinstance(first_option, bool):
                existing_fields_mapping = _update_field(
                    data_item, existing_fields_mapping
                )
    return existing_fields_mapping


def _update_field(
    data_item: Dict[str, Any],
    existing_fields_mapping: Optional[Dict[str, TFieldMapping]],
) -> Dict[str, TFieldMapping]:
    """Create or update the given field's info the custom fields state
    If the field hash already exists in the state from previous runs the name is not updated.
    New enum options (if any) are appended to the state.
    """
    existing_fields_mapping = existing_fields_mapping or {}
    key = data_item["key"]
    options = data_item.get("options", [])
    new_options_map = {str(o["id"]): o["label"] for o in options}
    existing_field = existing_fields_mapping.get(key)
    if not existing_field:
        existing_fields_mapping[key] = dict(
            name=data_item["name"],
            normalized_name=_normalized_name(data_item["name"]),
            options=new_options_map,
            field_type=data_item["field_type"],
        )
        return existing_fields_mapping
    existing_options = existing_field.get("options", {})
    if not existing_options or existing_options == new_options_map:
        existing_field["options"] = new_options_map
        existing_field["field_type"] = data_item[
            "field_type"
        ]  # Add for backwards compat
        return existing_fields_mapping
    # Add new enum options to the existing options array
    # so that when option is renamed the original label remains valid
    new_option_keys = set(new_options_map) - set(existing_options)
    for key in new_option_keys:
        existing_options[key] = new_options_map[key]
    existing_field["options"] = existing_options
    return existing_fields_mapping


def _normalized_name(name: str) -> str:
    source_schema = dlt.current.source_schema()
    normalized_name = name.strip()  # remove leading and trailing spaces
    return source_schema.naming.normalize_identifier(normalized_name)


def rename_fields(data: TDataPage, fields_mapping: Dict[str, Any]) -> TDataPage:
    if not fields_mapping:
        return data
    for data_item in data:
        for hash_string, field in fields_mapping.items():
            if hash_string not in data_item:
                continue
            field_value = data_item.pop(hash_string)
            field_name = field["name"]
            options_map = field["options"]
            # Get label instead of ID for 'enum' and 'set' fields
            if field_value and field["field_type"] == "set":  # Multiple choice
                field_value = [
                    options_map.get(str(enum_id), enum_id) for enum_id in field_value
                ]
            elif field_value and field["field_type"] == "enum":
                field_value = options_map.get(str(field_value), field_value)
            data_item[field_name] = field_value
    return data


def build_v2_fields_mapping(fields: TDataPage) -> Dict[str, TFieldMapping]:
    """Build a field mapping from Pipedrive API v2 field metadata."""
    fields_mapping: Dict[str, TFieldMapping] = {}
    for field in fields:
        if not field.get("is_custom_field"):
            continue
        field_code = field.get("field_code")
        if not field_code:
            continue
        fields_mapping[field_code] = {
            "name": field["field_name"],
            "normalized_name": _normalized_name(field["field_name"]),
            "options": _build_v2_options_map(field.get("options")),
            "field_type": field["field_type"],
        }
    return fields_mapping


def update_v2_fields_mapping(
    new_fields_mapping: TDataPage, existing_fields_mapping: Dict[str, Any]
) -> Dict[str, Any]:
    """Update custom fields state from Pipedrive API v2 field metadata."""
    for field in new_fields_mapping:
        if not field.get("is_custom_field"):
            continue
        field_code = field.get("field_code")
        if not field_code:
            continue
        data_item = {
            "key": field_code,
            "name": field["field_name"],
            "field_type": field["field_type"],
            "options": _build_v2_options(field.get("options")),
        }
        existing_fields_mapping = _update_field(data_item, existing_fields_mapping)
    return existing_fields_mapping


def rename_v2_custom_fields(
    data_item: Dict[str, Any], fields_mapping: Dict[str, Any]
) -> Dict[str, Any]:
    """Move mapped Pipedrive API v2 custom fields to readable top-level fields."""
    custom_fields = data_item.get("custom_fields")
    if not isinstance(custom_fields, dict) or not fields_mapping:
        return data_item
    data_item.pop("custom_fields")
    rename_fields([custom_fields], fields_mapping)
    data_item.update(custom_fields)
    return data_item


def _build_v2_options_map(options: Optional[Any]) -> Dict[str, str]:
    return {str(option["id"]): option["label"] for option in _build_v2_options(options)}


def _build_v2_options(options: Optional[Any]) -> TDataPage:
    if not options:
        return []
    if isinstance(options, dict):
        return [
            {"id": option_id, "label": label} for option_id, label in options.items()
        ]
    return cast(TDataPage, options)
