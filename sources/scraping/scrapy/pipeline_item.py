from typing import Type

import scrapy  # type: ignore
from scrapy.exceptions import CloseSpider  # type: ignore
from dlt.common import logger

from .spider import DltSpider


class ScrapingPipelineItem:
    def process_item(
        self, item: Type[scrapy.Item], spider: Type[DltSpider]
    ) -> Type[scrapy.Item]:
        """Handles sending the result of spider to scraping resource of Dlt pipeline"""
        if not spider.queue.is_closed:
            spider.queue.put(item)  # type: ignore
        else:
            logger.warning("Queue is closed")
            raise CloseSpider("Queue is closed, exiting...")

        return item
