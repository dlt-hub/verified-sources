from typing import Any
import dlt
import pytest

from scrapy.http import Response  # type: ignore

import sources.scraping.helpers

from sources.scraping import scrapy_source
from sources.scraping.helpers import create_pipeline_runner
from sources.scraping.scrapy.spider import DltSpider
from tests.utils import ALL_DESTINATIONS, load_table_counts

start_urls = ["https://quotes.toscrape.com/page/1/"]


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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="scraping",
        destination=destination_name,
        dataset_name="quotes",
    )

    pipeline_runner, scrapy_runner, wait = create_pipeline_runner(
        pipeline, spider=MySpider
    )

    pipeline_runner.run(
        scrapy_source(scrapy_runner.queue),
        write_disposition="replace",
        table_name="quotes",
    )

    scrapy_runner.run()
    wait()

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = {"quotes", "quotes__quote__tags"}
    assert set(table_counts.keys()) >= set(expected_tables)

    assert table_counts["quotes"] == 100
    assert table_counts["quotes__quote__tags"] == 232


def test_scrapy_pipeline_sends_data_in_queue(mocker):
    pipeline = dlt.pipeline(
        pipeline_name="scraping",
        destination="duckdb",
        dataset_name="quotes",
    )

    spy_on_queue = mocker.spy(sources.scraping.helpers.BaseQueue, "put")
    spy_on_queue_close = mocker.spy(sources.scraping.helpers.BaseQueue, "close")
    pipeline_runner, scrapy_runner, wait = create_pipeline_runner(
        pipeline,
        spider=MySpider,
        start_urls=start_urls,
    )

    pipeline_runner.run(
        scrapy_source(scrapy_runner.queue),
        write_disposition="replace",
        table_name="quotes",
    )

    scrapy_runner.run()
    wait()

    assert spy_on_queue.call_count == 100
    assert spy_on_queue_close.call_count == 1
