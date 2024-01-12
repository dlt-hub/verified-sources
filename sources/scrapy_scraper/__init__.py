"""Basic scrapy source"""
from queue import Queue
from typing import Iterable
import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

__all__ = ["scrapy_source"]


def get_scraping_results(queue: Queue) -> Iterable[TDataItem]:
    while True:
        result = queue.get()
        if "done" in result:
            break
        yield result


@dlt.source
def scrapy_source(
    queue: Queue,
    name: str | None = "scrapy",
) -> Iterable[DltResource]:
    """
    Source function for scraping links with Scrapy.

    Args:
        queue (Queue): Queue instance
        name (Queue | None): Optional name of resource

    Yields:
        DltResource: Scraped data from scrapy
    """
    yield dlt.resource(  # type: ignore
        get_scraping_results(queue),
        name=name,
        write_disposition="replace",
    )
