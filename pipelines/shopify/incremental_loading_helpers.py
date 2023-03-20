from datetime import datetime, timedelta
from dlt.common import pendulum
from dlt.common.configuration import with_config
from pendulum import Date, DateTime, Duration, Time
from time import sleep
from typing import Optional, Union

import dlt


@with_config
def get_since_timestamp(
    endpoint: str, datetime_field: str, step: int = dlt.config.value, max_retries: int = dlt.config.value, backoff_delay: float = dlt.config.value, days_back: int = dlt.config.value
) -> str:
    """
    Specific function to generate 'since_timestamp' string based on last timestamp stored in dlt's state/destiny (if available)
    """
    since_timestamp_str = ''
    if all([isinstance(step, int), step > 0, isinstance(days_back, int), days_back > 0]):
        last_timestamp = _get_last_timestamp_from_state(endpoint)
        if not last_timestamp:
            last_timestamp = _get_last_timestamp_from_destiny(endpoint, datetime_field, max_retries, backoff_delay)
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


def _get_last_timestamp_from_destiny(endpoint: str, datetime_field: str, max_retries: int, backoff_delay: float) -> Optional[Union[Date, Time, DateTime, Duration]]:
    last_timestamp = None
    if all([isinstance(max_retries, int), isinstance(backoff_delay, float)]):
        max_retries = abs(max_retries)
        backoff_delay = abs(backoff_delay)
        retries = 0
        source_schema = dlt.current.source_schema()
        normalized_endpoint = source_schema.naming.normalize_identifier(endpoint)
        sql_query = f"SELECT MAX({datetime_field}) AS last_timestamp FROM {normalized_endpoint}"
        while not last_timestamp and retries <= max_retries:
            try:
                current_pipeline = dlt.current.pipeline()  # type: ignore
                with current_pipeline.sql_client() as c:
                    with c.execute_query(sql_query) as cur:
                        row = cur.fetchone()
                        last_timestamp = pendulum.instance(row[0]) if row and isinstance(row[0], datetime) else None
            except Exception:
                sleep(backoff_delay)
            retries += 1
    return last_timestamp


def set_last_timestamp(endpoint: str, last_timestamp: str) -> None:
    """
    Specific function to store last timestamp in dlt's state
    """
    last_timestamps = dlt.state().setdefault('last_timestamps', {})
    last_timestamps[endpoint] = last_timestamp
