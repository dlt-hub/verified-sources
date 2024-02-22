import threading
import typing as t

import dlt
from dlt.common import logger
from pydispatch import dispatcher

from scrapy import signals, Item, Spider  # type: ignore
from scrapy.crawler import CrawlerProcess  # type: ignore

from .types import AnyDict, Runnable, P
from .queue import ScrapingQueue


class ScrapyRunner(Runnable):
    def __init__(
        self,
        spider: t.Type[Spider],
        start_urls: t.List[str],
        settings: AnyDict,
        on_item_scraped: t.Callable[[Item], None],
        on_engine_stopped: t.Callable[[], None],
    ) -> None:
        self.spider = spider
        self.settings = settings
        self.start_urls = start_urls

        # We want to receive on_item_scraped callback from
        # outside so we don't have to know about any queue instance.
        dispatcher.connect(on_item_scraped, signals.item_scraped)

        # Once crawling engine stops we would like to know about it as well.
        dispatcher.connect(on_engine_stopped, signals.engine_stopped)

    def run(self, *args: P.args, **kwargs: P.kwargs) -> t.Any:
        crawler = CrawlerProcess(settings=self.settings)
        crawler.crawl(
            self.spider,
            name="scraping_spider",
            start_urls=self.start_urls,
            **kwargs,
        )

        try:
            crawler.start()
        except Exception:
            logger.error("Was unable to start crawling process")
            self.on_engien_stopped()
            raise


class PipelineRunner(Runnable):
    def __init__(
        self,
        pipeline: dlt.Pipeline,
        queue: ScrapingQueue,
    ) -> None:
        self.pipeline = pipeline
        self.queue = queue
        self.scrapy_resource = dlt.resource(
            self.queue.get_batches,
            name=f"{self.pipeline_runner.pipeline.pipeline_name}_results",
        )

    def run(  # type: ignore[override]
        self,
        data: t.Any,
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
                self.pipeline.run(self.scrapy_resource, **kwargs)
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
        pipeline_worker = self.pipeline_runner.run(
            # Queue get_batches is a generator so we can
            # pass it to pipeline.run and dlt will handle the rest.
            *args,
            **kwargs,
        )

        logger.info("Starting scrapy crawler")
        self.scrapy_runner.run()

        # Wait to for pipeline finish it's job
        pipeline_worker.join()
