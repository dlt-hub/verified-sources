from typing import Any, Iterator, TypeVar
from queue import Empty

import dlt

from dlt.common import logger
from dlt.common.configuration.inject import with_config

from .helpers import ScrapingConfig
from .queue import BaseQueue


T = TypeVar("T")


def scrapy_source(queue: BaseQueue[T]) -> Iterator[Any]:
    yield scrapy_resource(queue)


@with_config(sections=("sources", "scraping"), spec=ScrapingConfig)
def scrapy_resource(
    queue: BaseQueue[T],
    batch_size: int = dlt.config.value,
    queue_result_timeout: int = dlt.config.value,
) -> Iterator[Any]:
    """Scrapy resource to retrieve scraped items from the queue

    Args:
        queue(BaseQueue[T]): Queue instance
        queue_result_timeout(int): timeout to wait for items in the queue

    Returns:
        Iterator[Any]: yields scraped items one by one
    """
    batch = []
    while True:
        # We want to make sure to consume all
        # items from queue before stopping
        if len(batch) >= batch_size:
            yield batch
            batch = []

        try:
            result = queue.get(timeout=queue_result_timeout)
            batch.append(result)
            queue.task_done()
        except Empty:
            logger.info(
                f"Queue has been empty for {queue_result_timeout}s, stopping..."
            )

            # Return the last batch before exiting
            if len(batch) > 0:
                yield batch

            break
