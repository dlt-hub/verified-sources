import threading
from typing import Callable, List, Type
from typing_extensions import TypeAlias
from dlt import Pipeline

from scrapy import Spider  # type: ignore
from scrapy.crawler import CrawlerRunner  # type: ignore
from twisted.internet import reactor

from .types import BaseQueue
from .settings import SOURCE_SCRAPY_SPIDER_SETTINGS

PipelineRunner: TypeAlias = Callable[[Pipeline, BaseQueue], None]


class Scraper:
    """Generic scraper wires up pipeline runner and `scrapy.CrawlerProcess` using `Queue`"""

    def __init__(
        self,
        pipeline: Pipeline,
        pipeline_runner: PipelineRunner,
        spider: Type[Spider],
        queue: BaseQueue,
        start_urls: List[str],
    ):
        self.pipeline = pipeline
        self.pipeline_runner = pipeline_runner
        self.spider = spider
        self.queue = queue
        self.start_urls = start_urls

    def start(self) -> None:
        runner = CrawlerRunner()
        runner.crawl(
            self.spider,
            queue=self.queue,
            name=f"{self.pipeline.pipeline_name}_spider",
            start_urls=self.start_urls,
            settings=SOURCE_SCRAPY_SPIDER_SETTINGS,
        )

        pipeline_thread_runner = threading.Thread(
            target=self.pipeline_runner,
            args=(
                self.pipeline,
                self.queue,
            ),
        )
        pipeline_thread_runner.start()

        d = runner.join()
        d.addBoth(lambda _: reactor.stop())
        reactor.run()

        pipeline_thread_runner.join()
