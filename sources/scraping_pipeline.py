from typing import Any

import dlt
from dlt.sources import DltResource
from scrapy import Spider
from scrapy.http import Response

from scraping import run_pipeline
from scraping.helpers import create_pipeline_runner


class MySpider(Spider):
    def parse(self, response: Response, **kwargs: Any) -> Any:
        for next_page in response.css("li.next a::attr(href)"):
            if next_page:
                yield response.follow(next_page.get(), self.parse)

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

    run_pipeline(
        pipeline,
        MySpider,
        # you can pass scrapy settings overrides here
        scrapy_settings={
            "DEPTH_LIMIT": 10,
        },
        write_disposition="append",
    )


def scrape_quotes_scrapy_configs() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="scraping_custom_scrapy_configs",
        destination="duckdb",
        dataset_name="quotes",
    )

    run_pipeline(
        pipeline,
        MySpider,
        # you can pass scrapy settings overrides here
        scrapy_settings={
            # How many sub pages to scrape
            # https://docs.scrapy.org/en/latest/topics/settings.html#depth-limit
            "DEPTH_LIMIT": 100,
            "SPIDER_MIDDLEWARES": {
                "scrapy.spidermiddlewares.depth.DepthMiddleware": 200,
                "scrapy.spidermiddlewares.httperror.HttpErrorMiddleware": 300,
            },
            "HTTPERROR_ALLOW_ALL": False,
        },
        write_disposition="append",
    )


def scrape_quotes_callback_access_resource() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="scraping_resource_callback",
        destination="duckdb",
        dataset_name="quotes",
    )

    def on_before_start(res: DltResource) -> None:
        res.add_limit(2)

    run_pipeline(
        pipeline,
        MySpider,
        batch_size=10,
        scrapy_settings={},
        on_before_start=on_before_start,
        write_disposition="append",
    )


def scrape_quotes_advanced_runner() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="scraping_advanced_direct",
        destination="duckdb",
    )
    scraping_host = create_pipeline_runner(pipeline, MySpider, batch_size=10)
    scraping_host.pipeline_runner.scraping_resource.add_limit(2)
    scraping_host.run(dataset_name="quotes", write_disposition="append")


if __name__ == "__main__":
    scrape_quotes()
    # scrape_quotes_scrapy_configs()
    # scrape_quotes_callback_access_resource()
    # scrape_quotes_advanced_runner()
