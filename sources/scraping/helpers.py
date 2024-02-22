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
from .types import StartUrls, P


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


def resolve_start_urls(path: StartUrls) -> t.List[str]:
    if isinstance(path, str):
        with open(path) as fp:
            return fp.readlines()

    return path or []


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def create_pipeline_runner(
    pipeline: dlt.Pipeline,
    spider: t.Type[Spider],
    queue_size: int = dlt.config.value,
    queue_result_timeout: int = dlt.config.value,
    start_urls: StartUrls = dlt.config.value,
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

    scrapy_runner = ScrapyRunner(
        queue=queue,
        spider=spider,
        start_urls=resolve_start_urls(start_urls),
        settings=SOURCE_SCRAPY_SETTINGS,
        on_item_scraped=on_item_scraped,
        on_engine_stopped=on_engine_stopped,
    )

    pipeline_runner = PipelineRunner(pipeline=pipeline)
    scraping_host = ScrapingHost(scrapy_runner, pipeline_runner)
    return scraping_host


def run_pipeline(
    pipeline: dlt.Pipeline,
    spider: t.Type[Spider],
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    """Simple runner for the scraping pipeline

    You can pass all parameters via kwargs to `dlt.pipeline.run(....)`

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
    scraping_host = create_pipeline_runner(pipeline, spider)
    scraping_host.run(*args, **kwargs)
