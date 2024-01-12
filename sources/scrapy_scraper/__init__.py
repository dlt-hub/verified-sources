"""Basic scrapy source"""
from typing import Iterable, Optional
import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .types import BaseQueue

__all__ = ["scrapy_source"]


def get_scraping_results(queue: BaseQueue) -> Iterable[TDataItem]:
    while True:
        result = queue.get()
        if "done" in result:
            break
        yield result


@dlt.source
def scrapy_source(
    queue: BaseQueue,
    name: Optional[str] = "scrapy",
) -> Iterable[DltResource]:
    """
    Source function for scraping links with Scrapy.

    Args:
        queue (BaseQueue): Queue instance
        name (BaseQueue | None): Optional name of resource

    Yields:
        DltResource: Scraped data from scrapy
    """
    yield dlt.resource(  # type: ignore
        get_scraping_results(queue),
        name=name,
        write_disposition="replace",
    )
