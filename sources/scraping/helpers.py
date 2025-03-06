import os
import typing as t

import dlt
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs.base_configuration import (
    configspec,
    BaseConfiguration,
)

from scrapy import Spider

from .queue import ScrapingQueue
from .settings import SOURCE_SCRAPY_QUEUE_SIZE, SOURCE_SCRAPY_SETTINGS
from .runner import ScrapingHost, PipelineRunner, ScrapyRunner, Signals
from .types import AnyDict


@configspec
class ScrapingConfig(BaseConfiguration):
    # Batch size for scraped items
    batch_size: int = 100

    # maxsize for queue
    queue_size: t.Optional[int] = SOURCE_SCRAPY_QUEUE_SIZE

    # result wait timeout for our queue
    queue_result_timeout: t.Optional[float] = 1.0

    # List of start urls
    start_urls: t.List[str] = None
    start_urls_file: str = None


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def resolve_start_urls(
    start_urls: t.Optional[t.List[str]] = dlt.config.value,
    start_urls_file: t.Optional[str] = dlt.config.value,
) -> t.List[str]:
    """Merges start urls
    If both `start_urls` and `start_urls_file` given, we will merge them
    and return deduplicated list of `start_urls` for scrapy spider.
    """
    urls = set()
    if os.path.exists(start_urls_file):
        with open(start_urls_file, encoding="utf-8") as fp:
            urls = {line for line in fp.readlines() if str(line).strip()}

    if start_urls:
        for url in start_urls:
            urls.add(url)

    return list(set(urls))


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def create_pipeline_runner(
    pipeline: dlt.Pipeline,
    spider: t.Type[Spider],
    batch_size: int = dlt.config.value,
    queue_size: int = dlt.config.value,
    queue_result_timeout: float = dlt.config.value,
    scrapy_settings: t.Optional[AnyDict] = None,
) -> ScrapingHost:
    """Creates scraping host instance
    This helper only creates pipeline host, so running and controlling
    scrapy runner and pipeline is completely delegated to advanced users
    """
    queue = ScrapingQueue(  # type: ignore
        maxsize=queue_size,
        batch_size=batch_size,
        read_timeout=queue_result_timeout,
    )

    signals = Signals(
        pipeline_name=pipeline.pipeline_name,
        queue=queue,
    )

    # Just to simple merge
    settings = {**SOURCE_SCRAPY_SETTINGS}
    if scrapy_settings:
        settings = {**scrapy_settings}

    scrapy_runner = ScrapyRunner(
        spider=spider,
        start_urls=resolve_start_urls(),
        signals=signals,
        settings=settings,
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
