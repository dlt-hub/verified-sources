from typing import Iterator, Sequence, Dict, Iterable, List, TypeVar, Any, Union, Tuple
from itertools import groupby, chain


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


def grouped_recents(pages: Iterable[Iterable[Dict[str, Any]]]) -> Iterator[Tuple[str, List[Dict[str, Any]]]]:
    for page in pages:
        for entity, items in groupby(sorted(page, key=_entity_group_key), key=_entity_group_key):
            yield entity, _extract_recents_data(items)
