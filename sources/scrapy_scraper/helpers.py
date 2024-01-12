import threading
from queue import Queue
from typing import Callable, List, Type, TypeAlias
from dlt import Pipeline

from scrapy import Spider
from scrapy.crawler import CrawlerProcess

from .settings import SOURCE_SCRAPY_SPIDER_SETTINGS

PipelineRunner: TypeAlias = Callable[[Pipeline, Queue], None]


class Scraper:
    """Generic scraper wires up pipeline runner and `scrapy.CrawlerProcess` using `Queue`"""

    def __init__(
        self,
        pipeline: Pipeline,
        pipeline_runner: PipelineRunner,
        spider: Type[Spider],
        queue: Queue,
        start_urls: List[str],
    ):
        self.pipeline = pipeline
        self.pipeline_runner = pipeline_runner
        self.spider = spider
        self.queue = queue
        self.start_urls = start_urls

    def start(self):
        process = CrawlerProcess()
        process.crawl(
            self.spider,
            queue=self.queue,
            name=f"{self.pipeline.pipeline_name}_spider",
            start_urls=self.start_urls,
            settings=SOURCE_SCRAPY_SPIDER_SETTINGS,
        )

        runner = threading.Thread(
            target=self.pipeline_runner,
            args=(
                self.pipeline,
                self.queue,
            ),
        )
        runner.start()
        process.start()
        runner.join()
