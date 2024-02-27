from typing import Any, Type, Union
import time
import threading

import dlt

from scrapy import Spider  # type: ignore
from scrapy.crawler import Crawler, CrawlerRunner  # type: ignore
from scrapy.http import Response  # type: ignore
from twisted.internet import reactor
from twisted.internet.defer import Deferred

from sources.scraping.queue import ScrapingQueue


start_urls = ["https://quotes.toscrape.com/page/1/"]
default_queue_batch_size = 100


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


class TestCrawlerProcess(CrawlerRunner):
    def crawl(
        self,
        crawler_or_spidercls: Union[Type[Spider] | str | Crawler],
        *args: Any,
        **kwargs: Any,
    ) -> Deferred:
        deferred = super().crawl(crawler_or_spidercls, *args, **kwargs)
        deferred.addBoth(lambda _: reactor.stop())
        return deferred

    def start(
        self, stop_after_crawl: bool = True, install_signal_handlers: bool = True
    ) -> None:
        try:
            reactor.run()
        except Exception:
            pass


def queue_closer(
    queue: ScrapingQueue, close_after_seconds: float = 1.0
) -> threading.Thread:
    def close_queue():
        time.sleep(close_after_seconds)
        queue.close()

    closer = threading.Thread(target=close_queue)
    closer.start()
    return closer


def table_expect_at_least_n_records(table_name: str, n: int, pipeline: dlt.Pipeline):
    with pipeline.sql_client() as client:
        with client.execute_query(f"SELECT * FROM {table_name}") as cursor:
            loaded_values = [item for item in cursor.fetchall()]
            assert len(loaded_values) == n
