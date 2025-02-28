"""This module contains abstractions to facilitate scraping and loading process"""
import threading
import typing as t
import dlt

from dlt.common import logger
from pydispatch import dispatcher
from typing_extensions import Self

from scrapy import signals, Item, Spider
from scrapy.crawler import CrawlerProcess

from .types import AnyDict, Runnable
from .queue import ScrapingQueue

T = t.TypeVar("T", bound=Item)


class Signals(t.Generic[T]):
    """Signals context wrapper

    This wrapper is also a callable which accepts `CrawlerProcess` instance
    this is required to stop the scraping process as soon as the queue closes.
    """

    def __init__(self, pipeline_name: str, queue: ScrapingQueue[T]) -> None:
        self.stopping = False
        self.queue = queue
        self.pipeline_name = pipeline_name

    def on_item_scraped(self, item: T) -> None:
        if not self.queue.is_closed:
            self.queue.put(item)
        else:
            logger.info(
                "Queue is closed, stopping",
                extra={"pipeline_name": self.pipeline_name},
            )
            if not self.stopping:
                self.on_engine_stopped()

    def on_engine_stopped(self) -> None:
        logger.info(f"Crawling engine stopped for pipeline={self.pipeline_name}")
        self.stopping = True
        self.crawler.stop()
        self.queue.close()
        self.queue.join()

    def __call__(self, crawler: CrawlerProcess) -> Self:
        self.crawler = crawler
        return self

    def __enter__(self) -> None:
        # We want to receive on_item_scraped callback from
        # outside so we don't have to know about any queue instance.
        dispatcher.connect(self.on_item_scraped, signals.item_scraped)

        # Once crawling engine stops we would like to know about it as well.
        dispatcher.connect(self.on_engine_stopped, signals.engine_stopped)

    def __exit__(self, exc_type: t.Any, exc_val: t.Any, exc_tb: t.Any) -> None:
        dispatcher.disconnect(self.on_item_scraped, signals.item_scraped)
        dispatcher.disconnect(self.on_engine_stopped, signals.engine_stopped)


class ScrapyRunner(Runnable):
    """Scrapy runner handles setup and teardown of scrapy crawling"""

    def __init__(
        self,
        spider: t.Type[Spider],
        start_urls: t.List[str],
        settings: AnyDict,
        signals: Signals[T],
    ) -> None:
        self.spider = spider
        self.start_urls = start_urls
        self.crawler = CrawlerProcess(settings=settings)
        self.signals = signals

    def run(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Runs scrapy crawler process

        All `kwargs` are forwarded to `crawler.crawl(**kwargs)`.
        Also manages relevant signal handling in proper way.
        """
        self.crawler.crawl(
            self.spider,
            name="scraping_spider",
            start_urls=self.start_urls,
            **kwargs,
        )

        try:
            logger.info("Starting the crawler")
            with self.signals(self.crawler):
                self.crawler.start()
        except Exception:
            logger.error("Was unable to start crawling process")
            raise
        finally:
            self.signals.on_engine_stopped()
            logger.info("Scraping stopped")


class PipelineRunner(Runnable):
    """Pipeline runner runs dlt pipeline in a separate thread
    Since scrapy wants to run in the main thread it is the only available
    option to host pipeline in a thread and communicate via the queue.
    """

    def __init__(self, pipeline: dlt.Pipeline, queue: ScrapingQueue[T]) -> None:
        self.pipeline = pipeline
        self.queue = queue

        if pipeline.dataset_name and not self.is_default_dataset_name(pipeline):
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

    def is_default_dataset_name(self, pipeline: dlt.Pipeline) -> bool:
        default_name = pipeline.pipeline_name + pipeline.DEFAULT_DATASET_SUFFIX
        return pipeline.dataset_name == default_name

    def run(
        self,
        *args: t.Any,
        **kwargs: t.Any,
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
        queue: ScrapingQueue[T],
        scrapy_runner: ScrapyRunner,
        pipeline_runner: PipelineRunner,
    ) -> None:
        self.queue = queue
        self.scrapy_runner = scrapy_runner
        self.pipeline_runner = pipeline_runner

    def run(
        self,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """You can pass kwargs which are passed to `pipeline.run`"""
        logger.info("Starting pipeline")
        pipeline_worker = self.pipeline_runner.run(*args, **kwargs)

        logger.info("Starting scrapy crawler")
        self.scrapy_runner.run()

        # Wait to for pipeline finish its job
        pipeline_worker.join()
