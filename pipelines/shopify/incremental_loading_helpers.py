from datetime import timedelta
from dlt.common import pendulum
from typing import Any, List

import dlt


def get_last_timestamp(endpoint: str, initial_days_back: int) -> Any:
    """
    Specific function to generate 'since_timestamp' string based on last timestamp stored in dlt's state (if available)
    """
    last_timestamp = get_last_metadatum_from_state(endpoint, 'timestamp')
    if not last_timestamp:
        if all([isinstance(initial_days_back, int), initial_days_back > 0]):
            last_timestamp = str(pendulum.now() - timedelta(days=initial_days_back))  # default value
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
