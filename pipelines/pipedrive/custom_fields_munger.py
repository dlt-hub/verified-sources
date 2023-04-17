from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

import dlt

from .typing import TDataPage


# def munge_push_func(data: Iterator[Dict[str, Any]], endpoint: str) -> Iterable[Dict[str, Any]]:
#     """
#     Specific function to perform data munging and push changes to custom fields' mapping stored in dlt's state
#     The endpoint must be an entity fields' endpoint
#     """
#     custom_fields_mapping = dlt.state().setdefault('custom_fields_mapping', {})
#     for data_item in data:
#         # 'edit_flag' field contains a boolean value, which is set to 'True' for custom fields and 'False' otherwise
#         if all([data_item.get('edit_flag'), data_item.get('name'), data_item.get('key')]):
#             # Regarding custom fields, 'key' field contains pipedrive's hash string representation of its name
#             # We assume that pipedrive's hash strings are meant to be an univoque representation of custom fields' name, so dlt's state shouldn't be updated while those values
#             # remain unchanged
#             # First of all, we search on and update dlt's state
#             if custom_fields_mapping.get(endpoint):
#                 if data_item['key'] not in custom_fields_mapping[endpoint]:
#                     data_item_mapping = _normalize_map(data_item)
#                     custom_fields_mapping[endpoint].update(data_item_mapping)
#             else:
#                 data_item_mapping = _normalize_map(data_item)
#                 custom_fields_mapping[endpoint] = data_item_mapping
#             # We end up updating data with dlt's state
#             data_item['key'] = custom_fields_mapping[endpoint][data_item['key']]['normalized_name']
#     return data


def update_fields_mapping(new_fields_mapping: TDataPage, existing_fields_mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    Specific function to perform data munging and push changes to custom fields' mapping stored in dlt's state
    The endpoint must be an entity fields' endpoint
    """
    for data_item in new_fields_mapping:
        # 'edit_flag' field contains a boolean value, which is set to 'True' for custom fields and 'False' otherwise
        if all([data_item.get('edit_flag'), data_item.get('name'), data_item.get('key')]):
            # Regarding custom fields, 'key' field contains pipedrive's hash string representation of its name
            # We assume that pipedrive's hash strings are meant to be an univoque representation of custom fields' name, so dlt's state shouldn't be updated while those values
            # remain unchanged
            # First of all, we search on and update dlt's state
            if existing_fields_mapping:
                if data_item['key'] not in existing_fields_mapping:
                    data_item_mapping = _normalize_map(data_item)
                    existing_fields_mapping.update(data_item_mapping)
            else:
                existing_fields_mapping = _normalize_map(data_item)
            # We end up updating data with dlt's state
            # data_item['key'] = existing_fields_mapping[data_item['key']]['normalized_name']
    return existing_fields_mapping


def _normalize_map(data_item: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    source_schema = dlt.current.source_schema()
    normalized_name = data_item['name'].strip()  # remove leading and trailing spaces
    normalized_name = source_schema.naming.normalize_identifier(normalized_name)
    return {
        data_item['key']: {
            'name': data_item['name'],
            'normalized_name': normalized_name
            }
    }


# def pull_munge_func(data: Iterator[Dict[str, Any]], endpoint: str) -> Iterable[Dict[str, Any]]:
#     """
#     Specific function to pull changes from custom fields' mapping stored in dlt's state and perform data munging
#     The endpoint must be an entity fields' endpoint
#     """
#     custom_fields_mapping = dlt.state().get('custom_fields_mapping')
#     if custom_fields_mapping:
#         data_item_mapping = custom_fields_mapping.get(endpoint)
#         if data_item_mapping:
#             rename_fields(data, data_item_mapping)
#     return data


def rename_fields(data: TDataPage, fields_mapping: Dict[str, Any]) -> TDataPage:
    if not fields_mapping:
        return data
    renames = [(hash_string, names["name"]) for hash_string, names in fields_mapping.items()]
    for data_item in data:
        for hash_string, name in renames:
            if hash_string in data_item:
                data_item[name] = data_item.pop(hash_string)
    return data
