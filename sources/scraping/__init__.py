"""Basic scrapy source"""
import logging

from typing import Callable, Iterable, List, Optional, Tuple, Type

import dlt

from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .helpers import init_scrapy_runner
from .settings import SOURCE_SCRAPY_SPIDER_SETTINGS
from .spider import DLTSpider, DLTSpiderBase
from .types import BaseQueue, OnNextPage, OnResult

__all__ = ["scrapy_source"]

logger = logging.getLogger(__file__)


def get_scraping_results(queue: BaseQueue) -> Iterable[TDataItem]:
    while True:
        result = queue.get()
        if "done" in result:
            break
        yield result


@dlt.source
def scrapy_source(
    name: Optional[str] = "scrapy",
    queue: Optional[BaseQueue] = None,
    spider: Optional[Type[DLTSpiderBase]] = None,
    on_result: Optional[OnResult] = None,
    on_next_page: Optional[OnNextPage] = None,
    queue_size: int = dlt.config.value,
    include_headers: bool = dlt.config.value,
    start_urls: List[str] = dlt.config.value,
) -> Iterable[DltResource]:
    """
    Source function for scraping links with Scrapy.

    Args:
        queue (BaseQueue): Queue instance
        name (BaseQueue | None): Optional name of resource

    Yields:
        DltResource: Scraped data from scrapy
    """
    if on_next_page and on_result:
        logger.info("Using on_next_page` and `on_result` callbacks")
        spider = DLTSpider
    elif spider and not (on_next_page or on_result):
        logger.info(f"Using custom spider {spider.__class__.__name__}")
    else:
        logger.error("Please provide spider or lifecycle hooks")
        raise RuntimeError(
            "Please provide spider class or otherwise specify"
            " `on_next_page` and `on_result` callbacks"
        )

    if queue is None:
        logger.info("Queue is not specified using defaul queue: queue.Queue")
        queue = BaseQueue(maxsize=queue_size)

    init_scrapy_runner(
        name=f"{name}_crawler",
        start_urls=start_urls,
        spider=spider,
        queue=queue,
        on_result=on_result,
        on_next_page=on_next_page,
        include_headers=include_headers,
        settings=SOURCE_SCRAPY_SPIDER_SETTINGS,
    )

    yield dlt.resource(  # type: ignore
        get_scraping_results(queue=queue),
        name=name,
        write_disposition="replace",
    )


def build_scrapy_source(
    name: Optional[str] = "scrapy",
    queue: Optional[BaseQueue] = None,
    spider: Optional[Type[DLTSpiderBase]] = None,
    on_result: Optional[OnResult] = None,
    on_next_page: Optional[OnNextPage] = None,
) -> Tuple[Callable[[None], None], Iterable[DltResource]]:
    if on_next_page and on_result:
        logger.info("Using on_next_page` and `on_result` callbacks")
        spider = DLTSpider
    elif spider and not (on_next_page or on_result):
        logger.info(f"Using custom spider {spider.__class__.__name__}")
    else:
        logger.error("Please provide spider or lifecycle hooks")
        raise RuntimeError(
            "Please provide spider class or otherwise specify"
            " `on_next_page` and `on_result` callbacks"
        )

    if queue is None:
        logger.info("Queue is not specified using defaul queue: queue.Queue")
        queue = BaseQueue(maxsize=dlt.config["sources.scraping"]["queue_size"])

    def scrapy_runner():
        init_scrapy_runner(
            name=f"{name}_crawler",
            start_urls=dlt.config["sources.scraping"]["start_urls"],
            spider=spider,
            queue=queue,
            on_result=on_result,
            on_next_page=on_next_page,
            include_headers=dlt.config["sources.scraping"]["queue_size"],
            settings=SOURCE_SCRAPY_SPIDER_SETTINGS,
        )

    @dlt.source(name=name)
    def scraper_source() -> Iterable[DltResource]:
        yield dlt.resource(  # type: ignore
            get_scraping_results(queue=queue),
            name=name,
        )

    return scrapy_runner, scraper_source
