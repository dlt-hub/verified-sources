"""Scraping source

Integrates Dlt and Scrapy to facilitate scraping pipelines.
"""
import inspect
import typing as t

import dlt

from dlt.sources import DltResource
from dlt.common.source import _SOURCES, SourceInfo

from scrapy import Spider  # type: ignore

from .helpers import ScrapingConfig, create_pipeline_runner
from .types import P, AnyDict


def run_pipeline(  # type: ignore[valid-type]
    pipeline: dlt.Pipeline,
    spider: t.Type[Spider],
    *args: P.args,
    on_before_start: t.Callable[[DltResource], None] = None,
    scrapy_settings: t.Optional[AnyDict] = None,
    batch_size: t.Optional[int] = None,
    queue_size: t.Optional[int] = None,
    queue_result_timeout: t.Optional[float] = None,
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


# This way we allow dlt init to detect scraping source it is indeed hacky
# and the core team is working to provide a better alternative.
_SOURCES[run_pipeline.__qualname__] = SourceInfo(
    ScrapingConfig,
    run_pipeline,
    inspect.getmodule(run_pipeline),
)
