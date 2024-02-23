"""Scraping source

Integrates Dlt and Scrapy to facilitate scraping pipelines.
"""
import inspect
import typing as t

import dlt
from dlt.common.source import _SOURCES, SourceInfo

from scrapy import Spider

from .helpers import ScrapingConfig, create_pipeline_runner
from .types import P, AnyDict


def run_pipeline(
    pipeline: dlt.Pipeline,
    spider: t.Type[Spider],
    *args: P.args,
    scrapy_settings: t.Optional[AnyDict],
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


_SOURCES[run_pipeline.__qualname__] = SourceInfo(
    ScrapingConfig,
    run_pipeline,
    inspect.getmodule(run_pipeline),
)
