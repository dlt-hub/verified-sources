"""Scraping source using scrapy"""
from typing import Any, Iterator, List, TypeVar
from queue import Empty

import dlt

from dlt.common import logger
from dlt.common.configuration.inject import with_config

from .helpers import ScrapingConfig
from .queue import BaseQueue, QueueClosedError


T = TypeVar("T")


@dlt.source
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
    batch: List[T] = []
    num_batches = 0
    while True:
        if len(batch) >= batch_size:
            num_batches += 1
            yield batch
            batch = []

        try:
            if queue.is_closed:
                raise QueueClosedError("Queue is closed")

            result = queue.get(timeout=queue_result_timeout)
            batch.append(result)

            # Mark task as completed
            queue.task_done()
        except Empty:
            logger.info(f"Queue has been empty for {queue_result_timeout}s...")

            # Return the current batch
            if batch:
                num_batches += 1
                yield batch
                batch = []
        except QueueClosedError:
            logger.info("Queue is closed, stopping...")

            # Return the last batch before exiting
            if batch:
                num_batches += 1
                yield batch

            logger.info(f"Loaded {num_batches} batches")

            break
