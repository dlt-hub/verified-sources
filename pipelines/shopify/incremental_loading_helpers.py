from datetime import datetime, timedelta, timezone
from dateutil import parser
from dlt.common.configuration import with_config
from enum import Enum
from time import sleep
from typing import Optional

import dlt
import re


class RelationType(str, Enum):
    NEXT = 'next'
    PREVIOUS = 'previous'


def get_linked_url(link_header: str, relation_type: str) -> str:
    """
    Specific function to extract next/previous linked url from the link header
    """
    linked_url = ''
    if all([isinstance(link_header, str), relation_type in tuple(RelationType)]):
        link_header_split = link_header.split(', ')
        link_header_filter = tuple(filter(lambda x: relation_type in x, link_header_split))
        if len(link_header_filter) == 1:
            linked_header_match = re.match(r'^.*<(.*)>.*$', link_header_filter[0])
            if linked_header_match:
                linked_url = linked_header_match.group(1)
    return linked_url


@with_config
def get_since_timestamp(endpoint: str, step: int = dlt.config.value, max_retries: int = dlt.config.value, backoff_delay: float = dlt.config.value, days_back: int = dlt.config.value) -> str:
    """
    Specific function to generate 'since_timestamp' based on last timestamp stored in dlt's state/destiny (if available)
    """
    since_timestamp_str = ''
    if all([isinstance(step, int), step > 0, isinstance(days_back, int), days_back > 0]):
        last_timestamp = _get_last_timestamp_from_state(endpoint)
        if not last_timestamp:
            last_timestamp = _get_last_timestamp_from_destiny(endpoint, max_retries, backoff_delay)
        if not last_timestamp:
            last_timestamp = datetime.now(timezone.utc) - timedelta(days=days_back)  # default value
        since_timestamp = last_timestamp + timedelta(seconds=step)
        since_timestamp_str = _parse_datetime_obj(since_timestamp)
    return since_timestamp_str


def _get_last_timestamp_from_state(endpoint: str) -> Optional[datetime]:
    last_timestamp_str = dlt.state().get('last_timestamps', {}).get(endpoint, '')
    last_timestamp = _parse_datetime_str(last_timestamp_str)
    return last_timestamp


def _get_last_timestamp_from_destiny(endpoint: str, max_retries: int, backoff_delay: float) -> Optional[datetime]:
    last_timestamp = None
    if all([isinstance(max_retries, int), isinstance(backoff_delay, float)]):
        max_retries = abs(max_retries)
        backoff_delay = abs(backoff_delay)
        retries = 0
        source_schema = dlt.current.source_schema()
        normalized_endpoint = source_schema.naming.normalize_identifier(endpoint)
        sql_query = f"SELECT MAX(created_at) AS last_timestamp FROM {normalized_endpoint}"
        while not last_timestamp and retries <= max_retries:
            try:
                current_pipeline = dlt.current.pipeline()  # type: ignore
                with current_pipeline.sql_client() as c:
                    with c.execute_query(sql_query) as cur:
                        row = cur.fetchone()
                        last_timestamp = row[0] if row and isinstance(row[0], datetime) else None
            except Exception:
                sleep(backoff_delay)
            retries += 1
    return last_timestamp


def _parse_datetime_obj(datetime_obj: datetime) -> str:
    datetime_str = ''
    if isinstance(datetime_obj, datetime):
        datetime_str = datetime.strftime(datetime_obj, '%Y-%m-%dT%H:%M:%S%z')
    return datetime_str


def _parse_datetime_str(datetime_str: str) -> Optional[datetime]:
    try:
        datetime_obj = parser.parse(datetime_str)
    except (OverflowError, parser.ParserError):
        datetime_obj = None
    return datetime_obj


def set_last_timestamp(endpoint: str, last_timestamp: str) -> None:
    """
    Specific function to store last timestamp in dlt's state
    """
    last_timestamps = dlt.state().setdefault('last_timestamps', {})
    last_timestamps[endpoint] = last_timestamp
