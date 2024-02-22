from typing import Any

import dlt
from scrapy import Spider  # type: ignore
from scrapy.http import Response  # type: ignore

from scraping import run_pipeline


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
        write_disposition="replace",
    )


if __name__ == "__main__":
    scrape_quotes()
