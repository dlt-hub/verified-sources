import threading
from typing import Any, Callable, Dict, List, Type

from scrapy import Spider  # type: ignore
from scrapy.crawler import CrawlerRunner  # type: ignore
from twisted.internet import reactor

from .types import BaseQueue


def init_scrapy_runner(
    name: str,
    start_urls: List[str],
    spider: Type[Spider],
    queue: BaseQueue,
    settings: Dict[str, Any],
    **kwargs,
) -> CrawlerRunner:
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
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


def start_pipeline(
    pipeline_runner: Callable[[], None],
    scrapy_runner: Callable[[], None],
):
    """Convenience method which handles the order of starting of pipeline and scrapy"""
    pipeline_thread_runner = threading.Thread(target=pipeline_runner)
    pipeline_thread_runner.start()
    scrapy_runner()
    pipeline_thread_runner.join()
