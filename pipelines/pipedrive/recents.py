import dlt

from typing import Iterator, Sequence, Dict, Iterable, List, TypeVar, Any, Union, Optional
from itertools import groupby, chain

from .typing import TDataPage
from .custom_fields_munger import rename_fields
from .helpers import _get_pages


T = TypeVar("T")


def list_wrapped(item: Union[List[T], T]) -> List[T]:
    if isinstance(item, list):
        return item
    return [item]


def _extract_recents_data(data: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Results from recents endpoint contain `data` key which is either a single entity or list of entities

    This returns a flat list of entities from an iterable of recent results
    """
    return list(chain.from_iterable((list_wrapped(item['data']) for item in data)))


def _entity_group_key(item: Dict[str, Any]) -> str:
    return item['item']  # type: ignore[no-any-return]


def _get_recent_pages(entity: str, pipedrive_api_key: str, since_timestamp: str) -> Iterator[TDataPage]:
    custom_fields_mapping = dlt.state().get('custom_fields_mapping', {}).get(entity, {})
    pages = _get_pages(
        "recents", pipedrive_api_key,
        extra_params=dict(since_timestamp=since_timestamp, items=entity),
    )
    pages = (_extract_recents_data(page) for page in pages)
    for page in pages:
        yield rename_fields(page, custom_fields_mapping)


def _get_recent_items_incremental(
    entity: str,
    pipedrive_api_key: str = dlt.secrets.value,
    since_timestamp: Optional[dlt.sources.incremental[str]] = dlt.sources.incremental('update_time|modified', '1970-01-01 00:00:00'),
) -> Iterator[TDataPage]:
    """Get a specific entity type from /recents with incremental state.
    """
    yield from _get_recent_pages(entity, pipedrive_api_key, since_timestamp.last_value)


def _get_recent_items(
    entity: str,
    pipedrive_api_key: str = dlt.secrets.value,
    since_timestamp: Optional[str] = None
) -> Iterator[TDataPage]:
    """Get all history for specific entity type from /recents.
    """
    yield from _get_recent_pages(entity, pipedrive_api_key, since_timestamp or "1970-01-01 00:00:00")


__source_name__ = 'sql_database'
