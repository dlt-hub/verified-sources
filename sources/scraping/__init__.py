from typing import Any, Iterator, TypeVar
from queue import Empty

import dlt

from dlt.common import logger
from dlt.common.configuration.inject import with_config

from .helpers import ScrapingConfig
from .types import BaseQueue

T = TypeVar("T")


def scrapy_source(queue: BaseQueue[T]) -> Iterator[Any]:
    yield scrapy_resource(queue)


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def scrapy_resource(
    queue: BaseQueue[T], queue_result_timeout: int = dlt.config.value
) -> Iterator[Any]:
    while True:
        # We want to make sure to consume all
        # items from queue before stopping
        try:
            result = queue.get(timeout=queue_result_timeout)
            yield result
            queue.task_done()
        except Empty:
            logger.info(
                f"Queue has been empty for {queue_result_timeout}s, stopping..."
            )
            break
