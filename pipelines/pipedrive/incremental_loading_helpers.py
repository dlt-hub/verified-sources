from datetime import datetime, timedelta
from dlt.common import pendulum
from typing import Any, Dict, List

import dlt

# https://developers.pipedrive.com/docs/api/v1/Recents
entities_endpoints = ('activities', 'activityTypes', 'deals', 'files', 'filters', 'notes', 'organizations', 'persons', 'pipelines', 'products', 'stages', 'users')
entity_items_params = ('activity', 'activityType', 'deal', 'file', 'filter', 'note', 'organization', 'person', 'pipeline', 'product', 'stage', 'user')
entities_mapping = dict(zip(entities_endpoints, entity_items_params))
entity_items_mapping = dict(zip(entity_items_params, entities_endpoints))


def get_entity_items_param(params: Dict[str, Any]) -> str:
    """
    Specific function to get 'recents' endpoint's items param
    """
    entity_items_param = ''
    if isinstance(params.get('items'), str):
        entity_items_params_split = params['items'].split(',')
        if len(entity_items_params_split) == 1:
            entity_items_param = entity_items_params_split[0]
    return entity_items_param


def get_last_timestamp(endpoint: str, initial_days_back: int, timestamp_format: str) -> Any:
    """
    Specific function to get last timestamp stored in dlt's state (if available)
    """
    last_timestamp = get_last_metadatum_from_state(endpoint, 'timestamp')
    if not last_timestamp:
        if all([isinstance(initial_days_back, int), initial_days_back > 0]):
            last_timestamp = datetime.strftime(pendulum.now() - timedelta(days=initial_days_back), timestamp_format)  # default value
    return last_timestamp


def get_last_metadatum_from_state(endpoint: str, metadatum: str) -> Any:
    last_metadata = _get_last_metadata_from_state(endpoint)
    return last_metadata.get(metadatum)


def _get_last_metadata_from_state(endpoint: str) -> Any:
    return dlt.state().get('last_metadata', {}).get(endpoint, {})


def set_last_metadata(endpoint: str, last_timestamp: str, last_ids: List[int]) -> None:
    """
    Specific function to store last metadata in dlt's state
    """
    last_metadata = dlt.state().setdefault('last_metadata', {})
    last_metadata[endpoint] = {'timestamp': last_timestamp, 'ids': last_ids}


def is_single_endpoint(endpoint: str) -> bool:
    return len(endpoint.split('/')) == 1
