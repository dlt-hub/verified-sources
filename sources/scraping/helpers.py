import threading
import typing as t
from typing import Any

import dlt
import scrapy  # type: ignore

from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs.base_configuration import (
    configspec,
    BaseConfiguration,
)

from scrapy.crawler import CrawlerProcess


from .types import AnyDict
from .queue import BaseQueue
from .scrapy.pipeline_item import get_item_pipeline
from .settings import SOURCE_SCRAPY_SETTINGS, SOURCE_SCRAPY_QUEUE_SIZE


T = t.TypeVar("T")


class Runnable(t.Protocol):
    def run(self, *args: t.Any, **kwargs: AnyDict) -> Any:
        pass


@configspec
class ScrapingConfig(BaseConfiguration):
    # Batch size for scraped items
    batch_size: int = 20

    # maxsize for queue
    queue_size: t.Optional[int] = SOURCE_SCRAPY_QUEUE_SIZE

    # result wait timeout for our queue
    queue_result_timeout: t.Optional[int] = 5

    # List of start urls
    start_urls: t.Optional[t.List[str]] = None


class ScrapyRunner(Runnable):
    def __init__(
        self,
        queue: BaseQueue[T],
        spider: t.Type[scrapy.Spider],
        start_urls: t.List[str],
        settings: AnyDict,
        include_headers: bool = False,
    ) -> None:
        self.queue = queue
        self.spider = spider
        self.settings = settings
        self.start_urls = start_urls
        self.include_headers = include_headers

    def run(self, *args: Any, **kwargs: AnyDict) -> None:
        crawler = CrawlerProcess(settings=self.settings)
        crawler.crawl(
            self.spider,
            queue=self.queue,
            include_headers=self.include_headers,
            name="scraping_spider",
            start_urls=self.start_urls,
            **kwargs,
        )

        crawler.start()

        # Join and close queue
        self.queue.join()
        self.queue.close()


class PipelineRunner(Runnable):
    def __init__(self, pipeline: dlt.Pipeline) -> None:
        self.pipeline = pipeline

    def run(  # type: ignore[override]
        self,
        data: Any,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """You can use all regular dlt.pipeline.run() arguments

        ```
        destination: TDestinationReferenceArg = None,
        staging: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        table_name: str = None,
        write_disposition: TWriteDisposition = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None
        ```
        """

        def run() -> None:
            self.pipeline.run(data, **kwargs)

        self.thread_runner = threading.Thread(target=run)
        self.thread_runner.start()

    def join(self) -> None:
        self.thread_runner.join()


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def create_pipeline_runner(
    pipeline: dlt.Pipeline,
    queue: t.Optional[BaseQueue[T]] = None,
    queue_size: int = dlt.config.value,
    spider: t.Type[scrapy.Spider] = None,
    start_urls: t.List[str] = dlt.config.value,
    include_headers: t.Optional[bool] = dlt.config.value,
) -> t.Tuple[PipelineRunner, ScrapyRunner, t.Callable[[], None]]:
    if queue is None:
        queue = BaseQueue(maxsize=queue_size)

    scrapy_pipeline_item = get_item_pipeline(queue)
    settings = {
        "ITEM_PIPELINES": {scrapy_pipeline_item: 100},
        **SOURCE_SCRAPY_SETTINGS,
    }

    scrapy_runner = ScrapyRunner(
        queue=queue,
        spider=spider,
        start_urls=start_urls,
        settings=settings,
        include_headers=include_headers,
    )

    pipeline_runner = PipelineRunner(pipeline=pipeline)

    def wait_for_results() -> None:
        pipeline_runner.join()

    return pipeline_runner, scrapy_runner, wait_for_results
