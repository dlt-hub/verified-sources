from datetime import datetime, timedelta, timezone
from dateutil import parser
from dlt.common.configuration import with_config
from time import sleep
from typing import Optional

import dlt

# https://developers.pipedrive.com/docs/api/v1/Recents
entities_endpoints = ('activities', 'activityTypes', 'deals', 'files', 'filters', 'notes', 'organizations', 'persons', 'pipelines', 'products', 'stages', 'users')
entity_items_params = ('activity', 'activityType', 'deal', 'file', 'filter', 'note', 'organization', 'person', 'pipeline', 'product', 'stage', 'user')
entities_mapping = dict(zip(entities_endpoints, entity_items_params))
entity_items_mapping = dict(zip(entity_items_params, entities_endpoints))


@with_config
def get_since_timestamp(endpoint: str, step: int = dlt.config.value, max_retries: int = dlt.config.value, backoff_delay: float = dlt.config.value, days_back: int = dlt.config.value) -> str:
    """
    Specific function to generate 'recents' endpoint's 'since_timestamp' based on last timestamp stored in dlt's state/destiny (if available)
    The endpoint must be an entities' endpoint
    """
    since_timestamp_str = ''
    if all([isinstance(step, int), step > 0, isinstance(days_back, int), days_back > 0]):
        last_timestamp = _get_last_timestamp_from_state(endpoint)
        if not last_timestamp:
            last_timestamp = _get_last_timestamp_from_destiny(endpoint, max_retries, backoff_delay)
        if last_timestamp:
            since_timestamp = last_timestamp + timedelta(seconds=step)
        else:
            since_timestamp = datetime.now(timezone.utc) - timedelta(days=days_back)  # default value
        since_timestamp_str = parse_datetime(since_timestamp)
    return since_timestamp_str


def parse_datetime(datetime_obj: datetime) -> str:
    datetime_str = ''
    if isinstance(datetime_obj, datetime):
        datetime_str = datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
    return datetime_str


def parse_datetime_str(datetime_str: str) -> Optional[datetime]:
    try:
        datetime_obj = parser.parse(datetime_str)
    except (OverflowError, parser.ParserError):
        datetime_obj = None
    return datetime_obj


UTC_OFFSET = '+00:00'


def _get_last_timestamp_from_state(endpoint: str) -> Optional[datetime]:
    last_timestamp_str = dlt.state().get('last_timestamps', {}).get(endpoint, '')
    if last_timestamp_str:
        last_timestamp = parse_datetime_str(last_timestamp_str + UTC_OFFSET)
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
                        last_timestamp = row[0] if row and isinstance(row[0], datetime) else None
            except:
                sleep(backoff_delay)
            retries += 1
    return last_timestamp


def set_last_timestamp(endpoint: str, last_timestamp_str: str) -> None:
    """
    Specific function to store last timestamp in dlt's state
    The endpoint must be an entities' endpoint
    """
    if endpoint in entities_mapping:
        last_timestamps = dlt.state().setdefault('last_timestamps', {})
        last_timestamps[endpoint] = last_timestamp_str


def max_datetime(first_datetime: Optional[datetime], second_datetime: Optional[datetime]) -> Optional[datetime]:
    if all([isinstance(first_datetime, (datetime, type(None))), isinstance(second_datetime, (datetime, type(None)))]):
        if first_datetime and second_datetime:
            return max(first_datetime, second_datetime)
        elif first_datetime:
            return first_datetime
        elif second_datetime:
            return second_datetime
