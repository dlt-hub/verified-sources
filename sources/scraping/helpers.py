import threading
import typing as t

import dlt
import scrapy  # type: ignore

from dlt.common import logger
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs.base_configuration import (
    configspec,
    BaseConfiguration,
)

from scrapy import signals, Item  # type: ignore
from scrapy.exceptions import CloseSpider  # type: ignore
from scrapy.crawler import CrawlerProcess  # type: ignore


from .types import AnyDict
from .queue import BaseQueue
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
        on_item_scraped: t.Callable[[Item], None],
        on_engine_stopped: t.Callable[[], None],
    ) -> None:
        self.queue = queue
        self.spider = spider
        self.settings = settings
        self.start_urls = start_urls
        self.on_item_scraped = on_item_scraped
        self.on_engien_stopped = on_engine_stopped

    def run(self, *args: t.Any, **kwargs: AnyDict) -> None:
        crawler = CrawlerProcess(settings=self.settings)
        crawler.crawl(
            self.spider,
            queue=self.queue,
            name="scraping_spider",
            start_urls=self.start_urls,
            **kwargs,
        )
        crawler.signals.connect(self.on_item_scraped, signals.item_scraped)
        crawler.signals.connect(self.on_engien_stopped, signals.engine_stopped)
        crawler.start()

class PipelineRunner(Runnable):
    def __init__(
        self,
        pipeline: dlt.Pipeline,
        queue: BaseQueue[T],
    ) -> None:
        self.pipeline = pipeline
        self.queue = queue

    def run(  # type: ignore[override]
        self,
        data: t.Any,
        *args: t.Any,
        **kwargs: t.Any,
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
            try:
                self.pipeline.run(data, **kwargs)
            except Exception:
                logger.error("Error during pipeline.run call, closing the queue")
                self.queue.close()
                raise

        self.thread_runner = threading.Thread(target=run)
        self.thread_runner.start()

    def join(self) -> None:
        self.thread_runner.join()


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def create_pipeline_runner(
    pipeline: dlt.Pipeline,
    queue_size: int = dlt.config.value,
    spider: t.Type[scrapy.Spider] = None,
    start_urls: t.List[str] = dlt.config.value,
) -> t.Tuple[PipelineRunner, ScrapyRunner, t.Callable[[], None]]:
    queue = BaseQueue(maxsize=queue_size)

    settings = {**SOURCE_SCRAPY_SETTINGS}

    def on_item_scraped(item: Item) -> None:
        if not queue.is_closed:
            queue.put(item)  # type: ignore
        else:
            logger.error("Queue is closed")
            raise CloseSpider("Queue is closed")

    def on_engine_stopped() -> None:
        queue.join()
        if not queue.is_closed:
            queue.close()

    scrapy_runner = ScrapyRunner(
        queue=queue,
        spider=spider,
        start_urls=start_urls,
        settings=settings,
        on_item_scraped=on_item_scraped,
        on_engine_stopped=on_engine_stopped,
    )

    pipeline_runner = PipelineRunner(pipeline=pipeline)

    def wait_for_results() -> None:
        pipeline_runner.join()

    return pipeline_runner, scrapy_runner, wait_for_results


def run_pipeline(
    pipeline: dlt.Pipeline,
    spider: t.Type[scrapy.Spider],
    urls: t.List[str],
) -> None:
    create_pipeline_runner(pipeline=pipeline, spider=spider, )
