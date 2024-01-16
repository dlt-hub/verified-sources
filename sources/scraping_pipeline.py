from typing import Callable, Dict, Generator, Iterable, Optional

import dlt
from dlt.sources import DltResource
from scrapy.http import Response

from scraping import build_scrapy_source
from scraping.helpers import start_pipeline


def parse(response: Response) -> Generator[Dict, None, None]:
    for quote in response.css("div.quote"):
        yield {
            "quote": {
                "text": quote.css("span.text::text").get(),
                "author": quote.css("small.author::text").get(),
                "tags": quote.css("div.tags a.tag::text").getall(),
            },
        }


def next_page(response: Response) -> Optional[str]:
    return response.css("li.next a::attr(href)").get()


def pipeline_runner(
    pipeline: dlt.Pipeline,
    source: Iterable[DltResource],
) -> Callable[[], None]:
    def run():
        load_info = pipeline.run(
            source,
            table_name="hello_world",
            write_disposition="replace",
        )
        print(load_info)

    return run


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bello",
        destination="duckdb",
        dataset_name="world",
    )

    scrapy_runner, scrapy_source = build_scrapy_source(
        on_result=parse,
        on_next_page=next_page,
    )

    start_pipeline(
        pipeline_runner(pipeline, scrapy_source()),
        scrapy_runner,
    )
