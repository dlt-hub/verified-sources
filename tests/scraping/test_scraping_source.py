from typing import Any
import time
import threading

import dlt
import pytest

from scrapy import Spider  # type: ignore
from scrapy.http import Response  # type: ignore

import sources.scraping.helpers

from sources.scraping import run_pipeline
from sources.scraping.helpers import create_pipeline_runner
from sources.scraping.queue import ScrapingQueue
from tests.utils import ALL_DESTINATIONS, load_table_counts

start_urls = ["https://quotes.toscrape.com/page/1/"]
default_queue_batch_size = 100


def queue_closer(
    queue: ScrapingQueue, close_after_seconds: float = 1.0
) -> threading.Thread:
    def close_queue():
        time.sleep(close_after_seconds)
        queue.close()

    closer = threading.Thread(target=close_queue)
    closer.start()
    return closer


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


def table_expect_at_least_n_records(table_name: str, n: int, pipeline: dlt.Pipeline):
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    print("TNS", table_names)
    with pipeline.sql_client() as client:
        with client.execute_query(f"SELECT * FROM {table_name}") as cursor:
            loaded_values = [item for item in cursor.fetchall()]
            assert len(loaded_values) == n


def test_scrapy_pipeline_sends_data_in_queue(mocker):
    pipeline = dlt.pipeline(
        pipeline_name="scraping",
        destination="duckdb",
        dataset_name="quotes",
    )

    spy_on_queue_put = mocker.spy(sources.scraping.helpers.ScrapingQueue, "put")
    spy_on_queue_close = mocker.spy(sources.scraping.helpers.ScrapingQueue, "close")
    run_pipeline(pipeline, MySpider)

    spy_on_queue_put.assert_called()
    spy_on_queue_close.assert_called()
    table_expect_at_least_n_records("scraping_results", 100, pipeline)


def test_scrapy_resource_yields_last_batch_on_queue_get_timeout():
    queue = ScrapingQueue(read_timeout=1.0, batch_size=5)
    queue.put({"n": 1})
    items = next(queue.get_batches())
    assert len(items) == 1


def test_scrapy_resource_yields_last_batch_if_queue_is_closed():
    queue = ScrapingQueue(read_timeout=1.0, batch_size=2)
    queue.put({"n": 1})
    queue.put({"n": 2})
    queue.put({"n": 3})
    queue_closer(queue, close_after_seconds=0.1)

    items = list(queue.get_batches())
    assert len(items) == 2


def test_queue_closed_if_pipeline_raises_an_exception(mocker):
    pipeline = dlt.pipeline(
        pipeline_name="scraping_exc",
        destination="duckdb",
        dataset_name="quotes",
    )

    spy_on_queue_close = mocker.spy(sources.scraping.helpers.ScrapingQueue, "close")
    run_pipeline(pipeline, MySpider, dataset_name="quotes")
    with mocker.patch("dlt.pipeline.pipeline.Pipeline.run", side_effect=OSError("bla")):
        spy_on_queue_close.assert_called()


def test_resource_add_limit_is_respected_by_queue_and_runners():
    pipeline = dlt.pipeline(
        pipeline_name="scraping_res_add_limit",
        destination="duckdb",
    )

    scraping_host = create_pipeline_runner(pipeline, MySpider, batch_size=10)
    scrapy_resource = scraping_host.pipeline_runner.scrapy_resource
    scraping_host.pipeline_runner.scrapy_resource = scrapy_resource.add_limit(2)
    scraping_host.run(dataset_name="quotes")
    table_expect_at_least_n_records("scraping_res_add_limit_results", 20, pipeline)
    table_expect_at_least_n_records(
        "scraping_res_add_limit__quote__tags", 232, pipeline
    )


@pytest.mark.forked
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_scraping_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="scraping_forked",
        destination=destination_name,
        dataset_name="quotes",
    )

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = {"scraping_results", "scraping_results__quote__tags"}
    assert set(table_counts.keys()) >= set(expected_tables)

    table_expect_at_least_n_records("scraping_results", 100, pipeline)
    table_expect_at_least_n_records("quotes__quote__tags", 232, pipeline)
