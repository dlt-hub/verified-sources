from typing import Any, Dict, Iterable, Iterator, List, Optional

import dlt


def munge_push_func(data: Iterator[Dict[str, Any]], endpoint: str) -> Iterable[Dict[str, Any]]:
    """
    Specific function to perform data munging and push changes to custom fields' mapping stored in dlt's state
    The endpoint must be an entity fields' endpoint
    """
    custom_fields_mapping = dlt.state().setdefault('custom_fields_mapping', {})
    for data_item in data:
        if all([data_item.get('edit_flag'), data_item.get('name'), data_item.get('key')]):
            if custom_fields_mapping.get(endpoint):
                if data_item['key'] not in custom_fields_mapping[endpoint]:
                    data_item_mapping = _normalize_map(data_item)
                    custom_fields_mapping[endpoint].update(data_item_mapping)
            else:
                data_item_mapping = _normalize_map(data_item)
                custom_fields_mapping[endpoint] = data_item_mapping
            data_item['key'] = custom_fields_mapping[endpoint][data_item['key']]['normalized_name']
    return data


def _normalize_map(data_item: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    source_schema = dlt.current.source_schema()
    normalized_name = data_item['name'].strip()  # remove leading and trailing spaces
    normalized_name = source_schema.naming.normalize_identifier(normalized_name)
    return {data_item['key']: {'name': data_item['name'], 'normalized_name': normalized_name}}


def pull_munge_func(data: Iterator[Dict[str, Any]], endpoint: str) -> Iterable[Dict[str, Any]]:
    """
    Specific function to pull changes from custom fields' mapping stored in dlt's state and perform data munging
    The endpoint must be an entity fields' endpoint
    """
    custom_fields_mapping = dlt.state().get('custom_fields_mapping')
    if custom_fields_mapping:
        for data_item in data:
            data_item_mapping = custom_fields_mapping.get(endpoint)
            if data_item_mapping:
                for hash_string, names in data_item_mapping.items():
                    if hash_string in data_item:
                        data_item[names['normalized_name']] = data_item.pop(hash_string)
    return data


@dlt.resource(name='custom_fields_mapping', write_disposition='replace')
def parsed_mapping() -> Optional[Iterator[List[Dict[str, str]]]]:
    """
    Parses and yields custom fields' mapping in order to be stored in destiny by dlt
    """
    custom_fields_mapping = dlt.state().get('custom_fields_mapping', {})
    for endpoint, data_item_mapping in custom_fields_mapping.items():
        yield [{'endpoint': endpoint, 'hash_string': hash_string, 'name': names['name'], 'normalized_name': names['normalized_name']} for hash_string, names in data_item_mapping.items()]
