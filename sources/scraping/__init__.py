"""Scraping source

Integrates Dlt and Scrapy to facilitate scraping pipelines.
"""
import typing as t

import dlt

from dlt.sources import DltResource

from scrapy import Spider

from .helpers import ScrapingConfig, create_pipeline_runner
from .types import AnyDict


def run_pipeline(
    pipeline: dlt.Pipeline,
    spider: t.Type[Spider],
    *args: t.Any,
    on_before_start: t.Callable[[DltResource], None] = None,
    scrapy_settings: t.Optional[AnyDict] = None,
    batch_size: t.Optional[int] = None,
    queue_size: t.Optional[int] = None,
    queue_result_timeout: t.Optional[float] = None,
    **kwargs: t.Any,
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
    options: AnyDict = {}
    if scrapy_settings:
        options["scrapy_settings"] = scrapy_settings

    if batch_size:
        options["batch_size"] = batch_size

    if queue_size:
        options["queue_size"] = queue_size

    if queue_result_timeout:
        options["queue_result_timeout"] = queue_result_timeout

    scraping_host = create_pipeline_runner(pipeline, spider, **options)

    if on_before_start:
        on_before_start(scraping_host.pipeline_runner.scraping_resource)

    scraping_host.run(*args, **kwargs)


@dlt.source(spec=ScrapingConfig)
def _register() -> DltResource:
    raise NotImplementedError(
        "Due to internal architecture of Scrapy, we could not represent it as a generator. Please use `run_pipeline` function instead"
    )
