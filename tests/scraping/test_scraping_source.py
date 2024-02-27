from unittest import mock

import dlt

import sources.scraping.helpers
import sources.scraping.queue

from sources.scraping import run_pipeline
from sources.scraping.helpers import create_pipeline_runner

from sources.scraping.queue import ScrapingQueue
from .utils import (
    MySpider,
    TestCrawlerProcess,
    queue_closer,
    table_expect_at_least_n_records,
)


@mock.patch("sources.scraping.runner.CrawlerProcess", TestCrawlerProcess)
def test_scrapy_pipeline_sends_data_in_queue(mocker):
    pipeline = dlt.pipeline(
        pipeline_name="scraping",
        destination="duckdb",
        dataset_name="quotes",
    )

    spy_on_queue_put = mocker.spy(sources.scraping.queue.ScrapingQueue, "put")
    spy_on_queue_close = mocker.spy(sources.scraping.queue.ScrapingQueue, "close")
    run_pipeline(pipeline, MySpider)

    spy_on_queue_put.assert_called()
    spy_on_queue_close.assert_called()
    table_expect_at_least_n_records("quotes", 100, pipeline)


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


@mock.patch("sources.scraping.runner.CrawlerProcess", TestCrawlerProcess)
def test_queue_closed_if_pipeline_raises_an_exception(mocker):
    pipeline = dlt.pipeline(
        pipeline_name="scraping_exc",
        destination="duckdb",
        dataset_name="quotes",
    )

    spy_on_queue_close = mocker.spy(sources.scraping.queue.ScrapingQueue, "close")
    with mocker.patch("dlt.pipeline.pipeline.Pipeline.run", side_effect=OSError("bla")):
        run_pipeline(pipeline, MySpider, dataset_name="quotes")
        spy_on_queue_close.assert_called()


# @mock.patch("sources.scraping.runner.CrawlerProcess", TestCrawlerProcess)
# def test_resource_add_limit_is_respected_by_queue_and_runners():
#     pipeline = dlt.pipeline(
#         pipeline_name="scraping_res_add_limit",
#         destination="duckdb",
#     )
#     scraping_host = create_pipeline_runner(pipeline, MySpider, batch_size=10)
#     scraping_host.pipeline_runner.scrapy_resource.add_limit(2)

#     scraping_host.run(write_disposition="replace")
#     table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
#     table_expect_at_least_n_records("scraping_res_add_limit_results", 20, pipeline)
#     table_expect_at_least_n_records("quotes__quote__tags", 68, pipeline)


# @pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
# @mock.patch("sources.scraping.runner.CrawlerProcess", TestCrawlerProcess)
# def test_scraping_all_resources(destination_name: str) -> None:
#     pipeline_name = f"scraping_forked_{destination_name}_results"
#     pipeline = dlt.pipeline(
#         pipeline_name=pipeline_name,
#         destination=destination_name,
#         dataset_name="quotes",
#     )

#     run_pipeline(
#         pipeline,
#         MySpider,
#         dataset_name="quotes",
#         write_disposition="replace",
#     )

#     table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
#     table_counts = load_table_counts(pipeline, *table_names)

#     # for now only check main tables
#     expected_tables = {
#         pipeline_name,
#         f"{pipeline_name}__quote__tags",
#     }
#     assert set(table_counts.keys()) >= set(expected_tables)

#     table_expect_at_least_n_records(pipeline_name, 100, pipeline)
#     table_expect_at_least_n_records(f"{pipeline_name}__quote__tags", 232, pipeline)
