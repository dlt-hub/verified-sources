from typing import Any

import dlt
from dlt.common import logger
from scrapy.http import Response  # type: ignore

from scraping import scrapy_source
from scraping.scrapy.spider import DltSpider
from scraping.helpers import create_pipeline_runner


class MySpider(DltSpider):
    def parse(self, response: Response, **kwargs: Any) -> Any:
        for next_page in response.css("li.next a::attr(href)"):
            if next_page:
                yield response.follow(next_page, self.parse)

        for quote in response.css("div.quote"):
            result = {
                "quote": {
                    "text": quote.css("span.text::text").get(),
                    "author": quote.css("small.author::text").get(),
                    "tags": quote.css("div.tags a.tag::text").getall(),
                },
            }
            yield result


def scrape_quotes() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="scraping",
        destination="duckdb",
        dataset_name="quotes",
    )

    pipeline_runner, scrapy_runner, wait = create_pipeline_runner(
        pipeline, spider=MySpider
    )

    logger.info("Starting pipeline")
    pipeline_runner.run(
        scrapy_source(scrapy_runner.queue),
        write_disposition="replace",
        table_name="quotes",
    )

    logger.info("Starting scrapy crawler")
    scrapy_runner.run()
    wait()


if __name__ == "__main__":
    scrape_quotes()
