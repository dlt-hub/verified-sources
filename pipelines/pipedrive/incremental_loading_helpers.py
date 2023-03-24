from datetime import datetime, timedelta
from dateutil import parser
from time import sleep
from typing import Optional

import dlt

# https://developers.pipedrive.com/docs/api/v1/Recents
entities_endpoints = ('activities', 'activityTypes', 'deals', 'files', 'filters', 'notes', 'organizations', 'persons', 'pipelines', 'products', 'stages', 'users')
entity_items_params = ('activity', 'activityType', 'deal', 'file', 'filter', 'note', 'organization', 'person', 'pipeline', 'product', 'stage', 'user')
entities_mapping = dict(zip(entities_endpoints, entity_items_params))
entity_items_mapping = dict(zip(entity_items_params, entities_endpoints))


def get_since_timestamp(endpoint: str, timedelta_seconds: int = 1, max_retries: int = 1, backoff_delay: float = .1) -> str:
    """
    Specific function to generate 'recents' endpoint's 'since_timestamp' based on last timestamp stored in dlt's state/destiny (if available)
    The endpoint must be an entities' endpoint
    """
    since_timestamp = ''
    if all([isinstance(timedelta_seconds, int), timedelta_seconds > 0]):
        last_timestamp = _get_last_timestamp_from_state(endpoint)
        if not last_timestamp:
            last_timestamp = _get_last_timestamp_from_destiny(endpoint, max_retries, backoff_delay)
        if last_timestamp:
            since_timestamp = last_timestamp + timedelta(seconds=timedelta_seconds)
            since_timestamp = datetime.strftime(since_timestamp, '%Y-%m-%d %H:%M:%S')
    return since_timestamp


def _parse_datetime_str(datetime_str: str) -> Optional[datetime]:
    try:
        datetime_obj = parser.parse(datetime_str)
    except (OverflowError, parser.ParserError):
        datetime_obj = None
    return datetime_obj


UTC_OFFSET = '+00:00'


def _get_last_timestamp_from_state(endpoint: str) -> Optional[datetime]:
    last_timestamp = dlt.state().get('last_timestamps', {}).get(endpoint, '')
    if last_timestamp:
        last_timestamp = _parse_datetime_str(last_timestamp + UTC_OFFSET)
        return last_timestamp


def _get_last_timestamp_from_destiny(endpoint: str, max_retries: int, backoff_delay: float) -> Optional[datetime]:
    last_timestamp = None
    if all([endpoint in entities_mapping, isinstance(max_retries, int), isinstance(backoff_delay, float)]):
        max_retries = abs(max_retries)
        backoff_delay = abs(backoff_delay)
        retries = 0
        source_schema = dlt.current.source_schema()
        normalized_endpoint = source_schema.naming.normalize_identifier(endpoint)
        sql_query = f"SELECT MAX(add_time) AS last_timestamp FROM {normalized_endpoint}"
        while not last_timestamp and retries <= max_retries:
            try:
                with dlt.current.pipeline().sql_client() as c:
                    with c.execute_query(sql_query) as cur:
                        row = cur.fetchone()
                        if row:
                            if isinstance(row[0], datetime):
                                last_timestamp = row[0]
                            elif isinstance(row[0], str):
                                last_timestamp = _parse_datetime_str(row[0])
            except:
                sleep(backoff_delay)
            retries += 1
    return last_timestamp


def set_last_timestamp(endpoint: str, last_timestamp: str) -> None:
    """
    Specific function to store last timestamp in dlt's state
    The endpoint must be an entities' endpoint
    """
    last_timestamps = dlt.state().setdefault('last_timestamps', {})
    if last_timestamps.get(endpoint):
        last_timestamps[endpoint] = last_timestamp
    else:
        last_timestamps.update({endpoint: last_timestamp})
