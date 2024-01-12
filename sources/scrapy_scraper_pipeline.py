from typing import List

import dlt

from scrapy_scraper import scrapy_source
from scrapy_scraper.helpers import Scraper
from scrapy_scraper.settings import SOURCE_SCRAPY_QUEUE_SIZE
from scrapy_scraper.spider import QuotesSpider
from scrapy_scraper.types import BaseQueue


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
    )

    scraper = Scraper(
        queue=result_queue,
        pipeline_runner=pipeline_runner,
        pipeline=pipeline,
        spider=QuotesSpider,
        start_urls=start_urls,
    )

    scraper.start()


if __name__ == "__main__":
    # please replace with you worn start urls
    # you wish to scrape
    start_urls = ["https://quotes.toscrape.com/page/1/"]
    load(start_urls)
