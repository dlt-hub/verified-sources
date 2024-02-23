from typing import Any

import dlt
from scrapy import Spider  # type: ignore
from scrapy.http import Response  # type: ignore

from scraping import run_pipeline
from scraping.helpers import create_pipeline_runner


class MySpider(Spider):
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
    )
    run_pipeline(
        pipeline,
        MySpider,
        scrapy_settings={},
        write_disposition="replace",
    )


def scrape_quotes_advanced_runner() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="scraping_advanced",
        destination="duckdb",
    )
    scraping_host = create_pipeline_runner(pipeline, MySpider, batch_size=10)
    scrapy_resource = scraping_host.pipeline_runner.scrapy_resource
    scraping_host.pipeline_runner.scrapy_resource = scrapy_resource.add_limit(20)
    scraping_host.run(dataset_name="quotes")


if __name__ == "__main__":
    scrape_quotes()
    # scrape_quotes_advanced_runner()
