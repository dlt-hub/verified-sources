from queue import Empty
from typing import Any, Iterator, List
import time
import threading

import dlt

from scrapy import Spider  # type: ignore
from scrapy.http import Response  # type: ignore

from sources.scraping.queue import QueueClosedError, ScrapingQueue


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


class MockQueue(ScrapingQueue):
    """Test queue alters the default get_batches behavior by
    adding max attempts count on queue read timeout
    """

    def __init__(
        self, maxsize: int = 0, batch_size: int = 10, read_timeout: float = 1.0
    ) -> None:
        super().__init__(maxsize, batch_size, read_timeout)
        self.max_empty_get_attempts = 5

    def get_batches(self) -> Iterator[Any]:
        batch: List = []
        get_attempts: int = 0
        while True:
            if len(batch) == self.batch_size:
                yield batch
                batch = []

            try:
                if self.is_closed:
                    try:
                        item = self.get_nowait()
                    except Empty:
                        raise QueueClosedError("Queue is closed")
                else:
                    item = self.get(timeout=self.read_timeout)
                batch.append(item)

                # Mark task as completed
                self.task_done()
            except Empty:
                if batch:
                    yield batch
                    batch = []

                if get_attempts >= self.max_empty_get_attempts:
                    self.close()
                    break

                print("Get attempt #", get_attempts)
                get_attempts += 1
            except QueueClosedError:
                if batch:
                    yield batch

                break


def queue_closer(
    queue: ScrapingQueue, close_after_seconds: float = 1.0
) -> threading.Thread:
    def close_queue():
        slept: int = 0
        while True:
            time.sleep(1)
            slept += 1
            if queue.is_closed:
                break

            if slept >= close_after_seconds:
                queue.close()
                break

    closer = threading.Thread(target=close_queue)
    closer.start()
    return closer


def table_expect_at_least_n_records(table_name: str, n: int, pipeline: dlt.Pipeline):
    with pipeline.sql_client() as client:
        with client.execute_query(f"SELECT count(*) FROM {table_name}") as cursor:
            count = cursor.fetchone()[0]
            assert count >= n, f"Expected at least {n} records, got {count}"
