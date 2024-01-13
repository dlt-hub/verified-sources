from queue import Queue
import dlt
import pytest
from sources.scrapy_scraper import scrapy_source
from sources.scrapy_scraper.helpers import Scraper
from sources.scrapy_scraper.spider import QuotesSpider
from sources.scrapy_scraper.types import BaseQueue
from tests.utils import ALL_DESTINATIONS, load_table_counts

start_urls = ["https://quotes.toscrape.com/page/1/"]


def pipeline_runner(pipeline: dlt.Pipeline, queue: BaseQueue) -> None:
    load_info = pipeline.run(
        scrapy_source(queue=queue),
        table_name="fam_quotes",
        write_disposition="replace",
    )

    print(load_info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    result_queue = Queue(maxsize=1000)
    pipeline = dlt.pipeline(
        pipeline_name="famous_quotes",
        destination="postgres",
        dataset_name="quotes",
        full_refresh=True,
    )

    scraper = Scraper(
        queue=result_queue,
        pipeline=pipeline,
        pipeline_runner=pipeline_runner,
        spider=QuotesSpider,
        start_urls=start_urls,
    )

    scraper.start()

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = {"fam_quotes", "fam_quotes__quote__tags"}
    assert set(table_counts.keys()) >= set(expected_tables)

    assert table_counts["fam_quotes"] == 100
    assert table_counts["fam_quotes__quote__tags"] == 232
