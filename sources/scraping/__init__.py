"""Basic scrapy source"""
import logging
from queue import Queue

from typing import Callable, Iterable, Optional, Tuple, Type, TypeVar

import dlt

from dlt.common.typing import TDataItem
from dlt.sources import DltResource, DltSource

from .helpers import init_scrapy_runner
from .settings import SOURCE_SCRAPY_SPIDER_SETTINGS, SOURCE_SCRAPY_QUEUE_SIZE
from .spider import DLTSpider, DLTSpiderBase
from .types import BaseQueue, OnNextPage, OnResult

__all__ = ["build_scrapy_source"]

logger = logging.getLogger(__file__)

T = TypeVar("T")


def get_scraping_results(queue: BaseQueue[T]) -> Iterable[TDataItem]:
    while True:
        result = queue.get()
        if isinstance(result, dict) and "done" in result:
            break
        yield result


def build_scrapy_source(
    name: Optional[str] = "scrapy",
    queue: Optional[BaseQueue[T]] = None,
    spider: Optional[Type[DLTSpiderBase]] = None,
    on_result: Optional[OnResult] = None,
    on_next_page: Optional[OnNextPage] = None,
) -> Tuple[Callable[[], None], Callable[[], DltSource]]:
    """Builder which configures scrapy and Dlt pipeline to run in and communicate using queue"""
    if not spider and not (on_next_page and on_result):
        logger.error("Please provide spider or lifecycle hooks")
        raise RuntimeError(
            "Please provide spider class or otherwise specify"
            " `on_next_page` and `on_result` callbacks"
        )

    if not spider:
        if not (on_next_page and on_result):
            raise RuntimeError(
                "Please specify `on_next_page` and `on_result` callbacks"
            )

        logger.info(
            "Using default generic spider with"
            " `on_next_page` and `on_result` callbacks"
        )
        spider = DLTSpider
    else:
        logger.info(f"Using custom spider {spider.__class__.__name__}")

    config = dlt.config["sources.scraping"]
    if queue is None:
        logger.info("Queue is not specified using defaul queue: queue.Queue")
        queue = Queue(maxsize=config.get("queue_size", SOURCE_SCRAPY_QUEUE_SIZE))

    def scrapy_runner() -> None:
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
