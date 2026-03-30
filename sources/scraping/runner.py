"""This module contains abstractions to facilitate scraping and loading process"""
import threading
import typing as t
import dlt

from dlt.common import logger
from twisted.internet import reactor as _reactor_module

import logging as _logging

from scrapy import signals, Item, Spider
from scrapy.crawler import Crawler, CrawlerRunner

from .types import AnyDict, Runnable
from .queue import ScrapingQueue

T = t.TypeVar("T", bound=Item)

_REACTOR: t.Any = _reactor_module
_REACTOR_THREAD: t.Optional[threading.Thread] = None
_REACTOR_LOCK = threading.Lock()


def _ensure_reactor() -> None:
    """Start the Twisted reactor in a daemon thread if not already running."""
    global _REACTOR_THREAD
    with _REACTOR_LOCK:
        if _REACTOR_THREAD is not None and _REACTOR_THREAD.is_alive():
            return

        started = threading.Event()

        def _run_reactor() -> None:
            _REACTOR.callWhenRunning(started.set)
            _REACTOR.run(installSignalHandlers=False)

        _REACTOR_THREAD = threading.Thread(target=_run_reactor, daemon=True)
        _REACTOR_THREAD.start()
        started.wait()


class Signals(t.Generic[T]):
    """Manages signal connections between a Scrapy Crawler and the scraping queue.

    Connects to spider_closed (not engine_stopped) to close the queue. This is
    important because spider_closed fires AFTER all item_scraped callbacks —
    guaranteed by the engine's deferred chain:
      scraper.close_spider() waits for slot.is_idle() → spider_closed signal
    Using engine_stopped would race with pending item_scraped callbacks.
    """

    def __init__(self, pipeline_name: str, queue: ScrapingQueue[T]) -> None:
        self._stopped = False
        self.queue = queue
        self.pipeline_name = pipeline_name
        self._crawler: t.Optional[Crawler] = None
        self._runner: t.Optional[CrawlerRunner] = None

    def connect(self, crawler: Crawler, runner: CrawlerRunner) -> None:
        """Connect signal handlers to a Crawler. Must be called from the reactor thread."""
        if self._crawler is not None:
            raise RuntimeError("Signals already connected to a crawler")
        self._crawler = crawler
        self._runner = runner
        crawler.signals.connect(self.on_item_scraped, signals.item_scraped)
        crawler.signals.connect(self.on_spider_closed, signals.spider_closed)

    def disconnect(self) -> None:
        """Disconnect signal handlers. Must be called from the reactor thread."""
        if self._crawler is not None:
            self._crawler.signals.disconnect(self.on_item_scraped, signals.item_scraped)
            self._crawler.signals.disconnect(
                self.on_spider_closed, signals.spider_closed
            )
            self._crawler = None
            self._runner = None

    def on_item_scraped(self, item: T) -> None:
        self.queue.put(item)

    def on_spider_closed(self, spider: t.Any, reason: str) -> None:
        logger.info(f"Spider closed for pipeline={self.pipeline_name} reason={reason}")
        self._initiate_stop()

    def _initiate_stop(self) -> None:
        """Idempotent stop: close the queue and stop the crawler.

        Uses reactor.callFromThread to stop the runner, which is safe
        from both the reactor thread and non-reactor threads.
        """
        if self._stopped:
            return
        self._stopped = True
        self.queue.close()
        if self._runner is not None:
            _REACTOR.callFromThread(self._runner.stop)


class ScrapyRunner(Runnable):
    """Scrapy runner handles setup and teardown of scrapy crawling.

    Uses a persistent Twisted reactor running in a daemon thread,
    allowing sequential pipeline runs in the same process.
    """

    def __init__(
        self,
        spider: t.Type[Spider],
        start_urls: t.List[str],
        settings: AnyDict,
        signals: Signals[T],
    ) -> None:
        self.spider = spider
        self.start_urls = start_urls
        self.signals = signals
        self.runner = CrawlerRunner(settings=settings)
        # apply scrapy log level without touching root handlers (dlt owns those)
        log_level = self.runner.settings.get("LOG_LEVEL", "WARNING")
        _logging.getLogger("scrapy").setLevel(log_level)
        _logging.getLogger("twisted").setLevel(_logging.ERROR)

    def run(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Runs scrapy crawler via the persistent reactor.

        Schedules the crawl in the reactor thread via callFromThread,
        then blocks until the crawl completes.
        """
        _ensure_reactor()

        done = threading.Event()
        crawl_error: t.List[BaseException] = []

        def _schedule() -> None:
            crawler = self.runner.create_crawler(self.spider)
            self.signals.connect(crawler, self.runner)

            d = self.runner.crawl(
                crawler,
                name="scraping_spider",
                start_urls=self.start_urls,
                **kwargs,
            )

            def _on_done(result: t.Any) -> None:
                self.signals.disconnect()
                done.set()

            def _on_error(failure: t.Any) -> None:
                self.signals.disconnect()
                crawl_error.append(failure.value)
                done.set()

            d.addCallback(_on_done)
            d.addErrback(_on_error)

        try:
            logger.info("Starting the crawler")
            _REACTOR.callFromThread(_schedule)
            done.wait()
        except Exception:
            logger.error("Was unable to start crawling process")
            raise
        finally:
            self.signals._initiate_stop()
            logger.info("Scraping stopped")

        if crawl_error:
            raise crawl_error[0]


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

        # Wait for pipeline to finish its job
        pipeline_worker.join(timeout=60)
        if pipeline_worker.is_alive():
            logger.warning(
                "Pipeline worker did not finish within timeout. Forcing queue close."
            )
            self.queue.close()
