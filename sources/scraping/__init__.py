"""Basic scrapy source"""
import logging

from typing import Callable, Iterable, Optional, Tuple, Type

import dlt

from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .helpers import init_scrapy_runner
from .settings import SOURCE_SCRAPY_SPIDER_SETTINGS, SOURCE_SCRAPY_QUEUE_SIZE
from .spider import DLTSpider, DLTSpiderBase
from .types import BaseQueue, OnNextPage, OnResult

__all__ = ["build_scrapy_source"]

logger = logging.getLogger(__file__)


def get_scraping_results(queue: BaseQueue) -> Iterable[TDataItem]:
    while True:
        result = queue.get()
        if "done" in result:
            break
        yield result


def build_scrapy_source(
    name: Optional[str] = "scrapy",
    queue: Optional[BaseQueue] = None,
    spider: Optional[Type[DLTSpiderBase]] = None,
    on_result: Optional[OnResult] = None,
    on_next_page: Optional[OnNextPage] = None,
) -> Tuple[Callable[[None], None], Iterable[DltResource]]:
    """Builder which configures scrapy and Dlt pipeline to run in and communicate using queue"""
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

    config = dlt.config["sources.scraping"]
    if queue is None:
        logger.info("Queue is not specified using defaul queue: queue.Queue")
        queue = BaseQueue(maxsize=config.get("queue_size", SOURCE_SCRAPY_QUEUE_SIZE))

    def scrapy_runner():
        init_scrapy_runner(
            name=f"{name}_crawler",
            start_urls=config.get("start_urls", []),
            spider=spider,
            queue=queue,
            on_result=on_result,
            on_next_page=on_next_page,
            include_headers=config.get("include_headers", False),
            settings=SOURCE_SCRAPY_SPIDER_SETTINGS,
        )

    @dlt.source(name=name)
    def scraper_source() -> Iterable[DltResource]:
        yield dlt.resource(  # type: ignore
            get_scraping_results(queue=queue),
            name=name,
        )

    return scrapy_runner, scraper_source
