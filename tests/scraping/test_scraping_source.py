from typing import Any
import time
import threading

import dlt
import pytest

from scrapy import Spider  # type: ignore
from scrapy.http import Response  # type: ignore

import sources.scraping.helpers

from sources.scraping import scrapy_resource, scrapy_source, logger
from sources.scraping.helpers import create_pipeline_runner
from sources.scraping.queue import BaseQueue
from tests.utils import ALL_DESTINATIONS, load_table_counts

start_urls = ["https://quotes.toscrape.com/page/1/"]


def queue_closer(
    queue: BaseQueue, close_after_seconds: float = 2.0
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


def test_scrapy_resource_yields_last_batch_on_queue_get_timeout():
    queue = BaseQueue()
    queue.put({"n": 1})

    items = next(
        scrapy_resource(
            queue=queue,
            queue_result_timeout=1,
            batch_size=5,
        )
    )

    assert len(items) == 1


def test_scrapy_resource_yields_everything_and_data_is_saved_to_destination():
    queue = BaseQueue()
    total_items = 25
    for i in range(total_items):
        queue.put({"n": i})

    res = scrapy_resource(queue=queue, queue_result_timeout=1, batch_size=10)
    p = dlt.pipeline("scrapy_example", full_refresh=True, destination="duckdb")

    closer = queue_closer(queue, close_after_seconds=0.2)
    p.run(res, table_name="numbers")
    closer.join()

    with p.sql_client() as client:
        with client.execute_query("SELECT * FROM numbers") as cursor:
            loaded_values = [item for item in cursor.fetchall()]
            assert len(loaded_values) == total_items


def test_scrapy_resource_yields_last_batch_when_queue_is_closed(mocker):
    queue = BaseQueue()
    total_items = 23
    for i in range(total_items):
        queue.put({"n": i})

    spy_on_logger = mocker.spy(logger, "info")
    res = scrapy_resource(
        queue=queue,
        queue_result_timeout=1,
        batch_size=5,
    )

    closer = queue_closer(queue, close_after_seconds=0.2)
    total_count = 0
    for batch in res:
        total_count += len(batch)

    closer.join()

    assert total_count == total_items
    assert spy_on_logger.called
    assert spy_on_logger.call_count >= 3
    assert spy_on_logger.call_args[0][0] == "Loaded 5 batches"

    # The last batch shoul only have 3 items
    assert len(batch) == 3


@pytest.mark.forked
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
