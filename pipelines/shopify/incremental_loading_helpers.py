from datetime import timedelta
from dlt.common import pendulum
from dlt.common.configuration import with_config
from pendulum import Date, DateTime, Duration, Time
from typing import Optional, Union

import dlt


@with_config
def get_since_timestamp(endpoint: str, step: int = dlt.config.value, days_back: int = dlt.config.value) -> str:
    """
    Specific function to generate 'since_timestamp' string based on last timestamp stored in dlt's state (if available)
    """
    since_timestamp_str = ''
    if all([isinstance(step, int), step > 0, isinstance(days_back, int), days_back > 0]):
        last_timestamp = _get_last_timestamp_from_state(endpoint)
        if not last_timestamp:
            last_timestamp = pendulum.now() - timedelta(days=days_back)  # default value
        since_timestamp_str = str(last_timestamp + timedelta(seconds=step))
    return since_timestamp_str


def _get_last_timestamp_from_state(endpoint: str) -> Optional[Union[Date, Time, DateTime, Duration]]:
    last_timestamp = None
    last_timestamp_str = dlt.state().get('last_timestamps', {}).get(endpoint, '')
    if last_timestamp_str:
        last_timestamp = pendulum.parse(last_timestamp_str)
    return last_timestamp


def set_last_timestamp(endpoint: str, last_timestamp_str: str) -> None:
    """
    Specific function to store last timestamp in dlt's state
    """
    last_timestamps = dlt.state().setdefault('last_timestamps', {})
    last_timestamps[endpoint] = last_timestamp_str
