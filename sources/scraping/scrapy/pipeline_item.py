from abc import ABC
from typing import Type, TypeVar

import scrapy  # type: ignore

from scrapy.exceptions import CloseSpider  # type: ignore
from dlt.common import logger

from ..queue import BaseQueue


T = TypeVar("T")


class PipelineItem(ABC):
    def process_item(
        self, item: Type[scrapy.Item], spider: scrapy.Spider
    ) -> Type[scrapy.Item]:
        raise NotImplementedError


def get_item_pipeline(queue: BaseQueue[T]) -> Type[PipelineItem]:
    """Wraps our custom ItemPipeline and provides queue instance

    It is done this way because there is no way to define
    custom initializer for scrapy ItemPipeline similar to closures.
    https://docs.scrapy.org/en/latest/topics/item-pipeline.html

    Args:
        queue (Type[BaseQueue]): queue instance

    Returns:
        (Type[PipelineItem]): Custom pipeline item with access to queue
    """

    class ScrapingPipelineItem(PipelineItem):
        def process_item(
            self, item: Type[scrapy.Item], spider: scrapy.Spider
        ) -> Type[scrapy.Item]:
            """Handles sending the result of spider to scraping resource of Dlt pipeline"""
            if not queue.is_closed:
                queue.put(item)  # type: ignore
            else:
                logger.error("Queue is closed")
                raise CloseSpider("Queue is closed")

            return item

    return ScrapingPipelineItem
