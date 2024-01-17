import logging
import threading
from typing import Any, Callable, Dict, List, Type, TypeVar

from scrapy import Spider  # type: ignore
from scrapy.crawler import CrawlerRunner  # type: ignore
from twisted.internet import reactor

from .types import BaseQueue

T = TypeVar("T")

logger = logging.getLogger(__name__)


def init_scrapy_runner(  # type: ignore[no-untyped-def]
    name: str,
    start_urls: List[str],
    spider: Type[Spider],
    queue: Type[BaseQueue],
    settings: Dict[str, Any],
    **kwargs,
) -> None:
    """Builds and prepares to run crawler with prepared configuration"""
    runner = CrawlerRunner()
    runner.crawl(
        spider,
        queue=queue,
        name=f"{name}_spider",
        start_urls=start_urls,
        settings=settings,
        **kwargs,
    )

    d = runner.join()
    d.addBoth(lambda _: reactor.stop())  # type: ignore[attr-defined]
    reactor.run()  # type: ignore[attr-defined]


def start_pipeline(
    pipeline_runner: Callable[[], None],
    scrapy_runner: Callable[[], None],
) -> None:
    """Convenience method which handles the order of starting of pipeline and scrapy"""
    logger.info("Starting scraping pipeline")
    pipeline_thread_runner = threading.Thread(target=pipeline_runner)
    pipeline_thread_runner.start()

    logger.info("Starting scrapy crawler runner")
    scrapy_runner()
    pipeline_thread_runner.join()
