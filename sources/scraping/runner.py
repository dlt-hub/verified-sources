import threading
import typing as t

import dlt
from dlt.common import logger
from pydispatch import dispatcher

from scrapy import signals, Item, Spider  # type: ignore
from scrapy.crawler import CrawlerProcess  # type: ignore
from scrapy.exceptions import CloseSpider  # type: ignore

from .types import AnyDict, Runnable, P
from .queue import ScrapingQueue


class Signals:
    def __init__(self, pipeline_name: str, queue: ScrapingQueue) -> None:
        self.queue = queue
        self.pipeline_name = pipeline_name

    def on_item_scraped(self, item: Item) -> None:
        if not self.queue.is_closed:
            self.queue.put(item)  # type: ignore
        else:
            logger.info(
                "Queue is closed ",
                extra={"pipeline_name": self.pipeline_name},
            )
            raise CloseSpider("Queue is closed")

    def on_spider_opened(self):
        if self.queue.is_closed:
            raise CloseSpider("Queue is closed")

    def on_engine_stopped(self) -> None:
        logger.info(f"Crawling engine stopped for pipeline={self.pipeline_name}")
        self.queue.join()
        self.queue.close()

    def __enter__(self):
        # There might be an edge case when Scrapy opens a new spider but
        # the queue has already been closed thus rendering endless wait
        dispatcher.connect(self.on_spider_opened, signals.spider_opened)

        # We want to receive on_item_scraped callback from
        # outside so we don't have to know about any queue instance.
        dispatcher.connect(self.on_item_scraped, signals.item_scraped)

        # Once crawling engine stops we would like to know about it as well.
        dispatcher.connect(self.on_engine_stopped, signals.engine_stopped)

    def __exit__(self, exc_type, exc_val, exc_tb):
        dispatcher.disconnect(self.on_spider_opened, signals.spider_opened)
        dispatcher.disconnect(self.on_item_scraped, signals.item_scraped)
        dispatcher.disconnect(self.on_engine_stopped, signals.engine_stopped)


class ScrapyRunner(Runnable):
    """Scrapy runner handles setup and teardown of scrapy crawling"""

    def __init__(
        self,
        spider: t.Type[Spider],
        start_urls: t.List[str],
        settings: AnyDict,
        signals: Signals,
    ) -> None:
        self.spider = spider
        self.start_urls = start_urls
        self.crawler = CrawlerProcess(settings=settings)
        self.signals = signals

    def run(self, *args: P.args, **kwargs: P.kwargs) -> t.Any:
        self.crawler.crawl(
            self.spider,
            name="scraping_spider",
            start_urls=self.start_urls,
            **kwargs,
        )

        try:
            logger.info("Starting the crawler")
            with self.signals:
                self.crawler.start()
        except Exception:
            logger.error("Was unable to start crawling process")
            raise
        finally:
            self.signals.on_engine_stopped()


class PipelineRunner(Runnable):
    """Pipeline runner runs dlt pipeline in a separate thread
    Since scrapy wants to run in the main thread it is the only available
    option to host pipeline in a thread and communicate via the queue.
    """

    def __init__(
        self,
        pipeline: dlt.Pipeline,
        queue: ScrapingQueue,
    ) -> None:
        self.pipeline = pipeline
        self.queue = queue

        if pipeline.dataset_name:
            resource_name = pipeline.dataset_name
        else:
            resource_name = f"{pipeline.pipeline_name}_results"

        logger.info(f"Resource name: {resource_name}")

        self.scraping_resource = dlt.resource(
            # Queue get_batches is a generator so we can
            # pass it to pipeline.run and dlt will handle the rest.
            self.queue.stream(),
            name=resource_name,
        )

    def run(  # type: ignore[override]
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> threading.Thread:
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
                self.pipeline.run(self.scraping_resource, **kwargs)
            except Exception:
                logger.error("Error during pipeline.run call, closing the queue")
                raise
            finally:
                self.queue.close()

        thread_runner = threading.Thread(target=run)
        thread_runner.start()
        return thread_runner


class ScrapingHost:
    """Scraping host runs the pipeline and scrapy"""

    def __init__(
        self,
        queue: ScrapingQueue,
        scrapy_runner: ScrapyRunner,
        pipeline_runner: PipelineRunner,
    ) -> None:
        self.queue = queue
        self.scrapy_runner = scrapy_runner
        self.pipeline_runner = pipeline_runner

    def run(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """You can pass kwargs which are passed to `pipeline.run`"""
        logger.info("Starting pipeline")
        pipeline_worker = self.pipeline_runner.run(*args, **kwargs)

        logger.info("Starting scrapy crawler")
        self.scrapy_runner.run()

        # Wait to for pipeline finish it's job
        pipeline_worker.join()
