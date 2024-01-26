import threading
import typing as t
from typing import Any

import dlt
import scrapy  # type: ignore

from dlt.common.schema.typing import TWriteDisposition
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs.base_configuration import (
    configspec,
    BaseConfiguration,
)

from scrapy.crawler import CrawlerRunner  # type: ignore
from twisted.internet import reactor

from .types import AnyDict
from .queue import BaseQueue
from .scrapy.spider import DltSpider
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
        runner = CrawlerRunner(settings=self.settings)
        runner.crawl(
            self.spider,
            queue=self.queue,
            include_headers=self.include_headers,
            name="scraping_spider",
            start_urls=self.start_urls,
            **kwargs,
        )

        d = runner.join()
        d.addBoth(lambda _: reactor.stop())  # type: ignore[attr-defined]
        reactor.run()  # type: ignore[attr-defined]


class PipelineRunner(Runnable):
    def __init__(self, pipeline: dlt.Pipeline) -> None:
        self.pipeline = pipeline

    def run(  # type: ignore[override]
        self,
        data: Any,
        *args: Any,
        write_disposition: t.Optional[TWriteDisposition] = "append",
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
            self.pipeline.run(data, write_disposition=write_disposition, **kwargs)

        self.thread_runner = threading.Thread(target=run)
        self.thread_runner.start()

    def join(self) -> None:
        self.thread_runner.join()


# We need to do this because scrapy requires passing
# module path instead of class type which it later
# resolves internally. Why this is done this way
# is because using inspect to get modulename etc.
# will even more complicate things and ScrapingPipelineItem
# at the moment is defined under `.scrapy.pipepline_item` is
# enough for use just to use `__package__` shortcut and
# manually construct module path. So if you move it somehwere else
# please provide it to runner.
ScrapyPipelineItemModule = f"{__package__}.scrapy.pipeline_item.ScrapingPipelineItem"


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def create_pipeline_runner(
    pipeline: dlt.Pipeline,
    queue: t.Optional[BaseQueue[T]] = None,
    queue_size: int = dlt.config.value,
    spider: t.Type[DltSpider] = None,
    start_urls: t.List[str] = dlt.config.value,
    include_headers: t.Optional[bool] = dlt.config.value,
) -> t.Tuple[PipelineRunner, ScrapyRunner, t.Callable[[], None]]:
    if queue is None:
        queue = BaseQueue(maxsize=queue_size)

    settings = {
        "ITEM_PIPELINES": {ScrapyPipelineItemModule: 100},
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
        queue.join()
        queue.close()
        pipeline_runner.join()

    return pipeline_runner, scrapy_runner, wait_for_results
