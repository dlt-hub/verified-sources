import os
import typing as t

import dlt
from dlt.common import logger
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs.base_configuration import (
    configspec,
    BaseConfiguration,
)


from scrapy import Item, Spider  # type: ignore
from scrapy.exceptions import CloseSpider  # type: ignore

from .settings import SOURCE_SCRAPY_QUEUE_SIZE, SOURCE_SCRAPY_SETTINGS
from .queue import ScrapingQueue
from .runner import ScrapingHost, PipelineRunner, ScrapyRunner
from .types import StartUrls, StartUrlsFile


@configspec
class ScrapingConfig(BaseConfiguration):
    # Batch size for scraped items
    batch_size: int = 20

    # maxsize for queue
    queue_size: t.Optional[int] = SOURCE_SCRAPY_QUEUE_SIZE

    # result wait timeout for our queue
    queue_result_timeout: t.Optional[int] = 5

    # List of start urls
    start_urls: StartUrls = None
    start_urls_file: StartUrlsFile = None


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def resolve_start_urls(
    start_urls: StartUrls = dlt.config.value,
    start_urls_file: StartUrlsFile = dlt.config.value,
) -> t.List[str]:
    urls = set()
    if os.path.exists(start_urls_file):
        with open(start_urls_file) as fp:
            urls = {line for line in fp.readlines() if str(line).strip()}

    if start_urls:
        for url in start_urls:
            urls.add(url)

    return list(set(urls))


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def create_pipeline_runner(
    pipeline: dlt.Pipeline,
    spider: t.Type[Spider],
    queue_size: int = dlt.config.value,
    queue_result_timeout: int = dlt.config.value,
) -> ScrapingHost:
    queue = ScrapingQueue(
        maxsize=queue_size,
        read_timeout=queue_result_timeout,
    )

    def on_item_scraped(item: Item) -> None:
        if not queue.is_closed:
            queue.put(item)  # type: ignore
        else:
            logger.error("Queue is closed")
            raise CloseSpider("Queue is closed")

    def on_engine_stopped() -> None:
        queue.join()
        queue.close()

    settings = {
        **SOURCE_SCRAPY_SETTINGS,
        "LOG_LEVEL": logger.log_level(),
    }
    scrapy_runner = ScrapyRunner(
        spider=spider,
        start_urls=resolve_start_urls(),
        settings=settings,
        on_item_scraped=on_item_scraped,
        on_engine_stopped=on_engine_stopped,
    )

    pipeline_runner = PipelineRunner(
        pipeline=pipeline,
        queue=queue,
    )

    scraping_host = ScrapingHost(
        queue,
        scrapy_runner,
        pipeline_runner,
    )

    return scraping_host
