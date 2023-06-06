"""Shopify source helpers"""

from dlt.common.typing import TDataItem
from typing import Any, Iterable


def iterate_page(page: Any) -> Iterable[TDataItem]:
    """
    Iterates over all pages yielding all items

    Args:
        page (Any): The starting page object.

    Yields:
        TDataItem: Each item from the pages.

    Returns:
        Iterable[TDataItem]: An iterable of items from the pages.
    """
    while page:
        for item in page:
            yield item.to_dict()
        page = page.next_page() if page.has_next_page() else None
