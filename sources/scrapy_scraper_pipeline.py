from typing import Generator, List, Optional

import dlt
import scrapy  # type: ignore

from scrapy.responsetypes import Response  # type: ignore

from scrapy_scraper import scrapy_source
from scrapy_scraper.helpers import Scraper
from scrapy_scraper.settings import SOURCE_SCRAPY_QUEUE_SIZE
from scrapy_scraper.spider import DLTSpiderBase
from scrapy_scraper.types import BaseQueue


class QuotesSpider(DLTSpiderBase):
    """
    This is a slightly modified example of Scrapy spider
    which you find in the official documentation
    https://docs.scrapy.org/en/latest/intro/tutorial.html#extracting-data-in-our-spider.

    To send the data from the spider to pipeline the spider needs a queue instance,
    once scrapy closes the session we also send a termination flag so
    DLT pipeline can stop waiting for more data."""

    def parse(
        self, response: Response
    ) -> Optional[Generator[scrapy.Request, None, None]]:
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


def pipeline_runner(pipeline: dlt.Pipeline, queue: BaseQueue) -> None:
    load_info = pipeline.run(
        scrapy_source(queue=queue),
        table_name="fam_quotes",
        write_disposition="replace",
    )

    print(load_info)


def load(start_urls: List[str]) -> None:
    result_queue = BaseQueue(maxsize=SOURCE_SCRAPY_QUEUE_SIZE)
    pipeline = dlt.pipeline(
        pipeline_name="famous_quotes",
        destination="postgres",
        dataset_name="quotes",
    )

    scraper = Scraper(
        queue=result_queue,
        pipeline=pipeline,
        pipeline_runner=pipeline_runner,
        spider=QuotesSpider,
        start_urls=start_urls,
    )

    scraper.start()


if __name__ == "__main__":
    # please replace with you worn start urls
    # you wish to scrape
    start_urls = ["https://quotes.toscrape.com/page/1/"]
    load(start_urls)
