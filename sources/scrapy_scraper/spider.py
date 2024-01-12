from queue import Queue
from typing import Any, List

import scrapy


class DLTSpiderBase(scrapy.Spider):
    """Base spider which requires additional
    dependencies and subsequent spiders should
    implement their own parse logic with sending
    the extracted data over the queue.
    """

    def __init__(
        self,
        name: str | None = None,
        queue: Queue | None = None,
        settings: dict | None = {},
        start_urls: List[str] | None = [],
        **kwargs: Any,
    ):
        super().__init__(name, **kwargs)
        self._queue = queue
        self.custom_settings = settings
        self.start_urls = start_urls

    def send_data(self, data: Any):
        self._queue.put(data)

    def done(self):
        self.send_data({"done": True})


"""
This is a slightly modified example of Scrapy spider
which you find in the official documentation
https://docs.scrapy.org/en/latest/intro/tutorial.html#extracting-data-in-our-spider.

To send the data from the spider to pipeline the spider needs a queue instance,
once scrapy closes the session we also send a termination flag so
DLT pipeline can stop waiting for more data.
"""


class QuotesSpider(DLTSpiderBase):
    def parse(self, response):
        for quote in response.css("div.quote"):
            data = {
                "headers": {
                    "status": response.status,
                    **dict(response.headers.to_unicode_dict()),
                },
                "quote": {
                    "text": quote.css("span.text::text").get(),
                    "author": quote.css("small.author::text").get(),
                    "tags": quote.css("div.tags a.tag::text").getall(),
                },
            }

            # Once we have our data we need to send
            # it over the queue to pipeline
            self.send_data(data)

        # Find the next page and create next request otherwise
        # send termination info.
        next_page = response.css("li.next a::attr(href)").get()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
        else:
            # Important: once we are done we need to send signal
            # to pipeline so it can finalize and shutdown
            self.done()
