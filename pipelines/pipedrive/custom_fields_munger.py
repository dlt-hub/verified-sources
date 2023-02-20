from dlt.common.normalizers.names.snake_case import normalize_column_name
from enum import Enum
from typing import Dict, Generator, Tuple, Union

import dlt
import json
import functools

_custom_fields_mapping = {}


class EndpointType(Enum):
    ENTITY = 'ENTITY'
    ENTITY_FIELDS = 'ENTITY_FIELDS'


def get_munge_endpoint(endpoint: str = '', pipedrive_api_key: str = dlt.secrets.value, extra_params: Dict = None, endpoint_type: EndpointType = None) -> Generator[Dict, None, None]:
    """
    Generic function to retrieve and munge endpoint data.
    """

    from . import get_endpoint  # workaround

    if all([endpoint, endpoint_type in EndpointType]):
        data_pages = get_endpoint(endpoint, pipedrive_api_key, extra_params=extra_params)
        munge_func = _munge_push_func if endpoint_type == EndpointType.ENTITY_FIELDS else _pull_munge_func
        for data_page in data_pages:
            data_page_map = map(functools.partial(munge_func, endpoint=endpoint), data_page)
            yield from data_page_map


def _munge_push_func(data_item: Dict = None, endpoint: str = '') -> Dict:
    """
    Specific function to munge data and push changes to custom fields' mapping
    The endpoint must be an entity fields' endpoint
    """
    if all([data_item, data_item.get('edit_flag'), data_item.get('name'), data_item.get('key'), endpoint]):
        normalized_name = data_item['name'].strip()  # remove leading and trailing spaces (e.g. 'MULTIPLE OPTIONS ' -> 'multiple_options_')
        normalized_name = normalized_name.lower()  # typos' workaround (e.g. 'CUSTOMFIELD_DEaL_USER' -> 'customfield_d_ea_l_user')
        normalized_name = normalize_column_name(normalized_name)
        data_item_mapping = {data_item['key']: normalized_name}
        if _custom_fields_mapping.get(endpoint):
            _custom_fields_mapping[endpoint].update(data_item_mapping)
        else:
            _custom_fields_mapping[endpoint] = data_item_mapping
        data_item['key'] = normalized_name
    return data_item


def _pull_munge_func(data_item: Dict = None, endpoint: str = '') -> Dict:
    """
    Specific function to pull changes from custom fields' mapping and munge data
    The endpoint must be an entity's endpoint
    """
    if all([data_item, endpoint]):
        endpoint = f'{endpoint[:-1]}Fields'  # converts entity's endpoint into entity fields' endpoint
        data_item_mapping = _custom_fields_mapping.get(endpoint)
        if data_item_mapping:
            for hash_string, normalized_name in data_item_mapping.items():
                if data_item.get(hash_string, KeyError) is not KeyError:  # only check key existence
                    data_item[normalized_name] = data_item.pop(hash_string)
    return data_item


def get_parse_mapping() -> Generator[Dict, None, None]:
    """
    Specific function to parse custom fields' mapping, in order to be stored by dlt
    """
    for endpoint, data_item_mapping in _custom_fields_mapping.items():
        for hash_string, normalized_name in data_item_mapping.items():
            yield {endpoint: [{'key': normalized_name, 'hash_string': hash_string}]}


class MappingFormat(Enum):
    JSON = 'JSON'


def get_mapping(mapping_format: MappingFormat = None) -> Union[str, Dict]:
    if mapping_format == MappingFormat.JSON:
        return _mapping_json()
    return _custom_fields_mapping


def _mapping_json(sort_keys: bool = True, indent: int = 4, separators: Tuple = (',', ': ')) -> str:
    return json.dumps(_custom_fields_mapping, sort_keys=sort_keys, indent=indent, separators=separators)
