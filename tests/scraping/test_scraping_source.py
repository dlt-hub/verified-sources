from unittest import mock

import dlt
import pytest

from twisted.internet import reactor

from sources.scraping import run_pipeline
from sources.scraping.helpers import create_pipeline_runner
from sources.scraping.queue import ScrapingQueue
from sources.scraping.runner import PipelineRunner

from tests.utils import ALL_DESTINATIONS, load_table_counts

from .utils import (
    MySpider,
    TestCrawlerProcess,
    queue_closer,
    table_expect_at_least_n_records,
)


@pytest.fixture(scope="module")
def shutdown_reactor():
    yield True

    try:
        reactor.stop()
    except Exception:  # noqa
        pass


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
def test_pipeline_runners_handle_extended_and_simple_use_cases(
    mocker, shutdown_reactor
):
    pipeline = dlt.pipeline(
        pipeline_name="scraping_res_add_limit",
        destination="duckdb",
    )

    spy_on_queue_put = mocker.spy(ScrapingQueue, "put")
    spy_on_queue_close = mocker.spy(ScrapingQueue, "close")
    spy_on_crawler_process = mocker.spy(TestCrawlerProcess, "stop")
    scraping_host = create_pipeline_runner(pipeline, MySpider, batch_size=10)
    scraping_host.pipeline_runner.scraping_resource.add_limit(2)

    # Make sure we close the queue to let the scraping
    # to properly shut down in testing machine and exit
    queue_closer(scraping_host.queue, close_after_seconds=30)
    scraping_host.run(dataset_name="quotes", write_disposition="append")

    table_expect_at_least_n_records("scraping_res_add_limit_results", 20, pipeline)
    table_expect_at_least_n_records(
        "scraping_res_add_limit_results__quote__tags", 68, pipeline
    )

    spy_on_queue_put.assert_called()
    spy_on_queue_close.assert_called()
    spy_on_crawler_process.assert_called()

    err_pipeline = dlt.pipeline(
        pipeline_name="scraping_exc",
        destination="duckdb",
        dataset_name="quotes",
    )

    with mocker.patch("dlt.Pipeline.run", side_effect=OSError("bla")):
        run_pipeline(err_pipeline, MySpider, dataset_name="quotes")
        spy_on_queue_close.assert_called()


def test_resource_name_assignment_and_generation():
    queue = ScrapingQueue()
    # If dataset_name is given to pipeline then we will have
    # resource name same as dataset_name
    pipeline1 = dlt.pipeline(
        pipeline_name="pipeline_one",
        destination="duckdb",
        dataset_name="cookies",
    )
    pipeline_runner = PipelineRunner(
        pipeline=pipeline1,
        queue=queue,
    )
    assert pipeline_runner.scraping_resource.name == "cookies"

    # If datasert_name is not given to pipeline then we will have
    # resource name is generate like pipeline.name + "_result" suffix
    pipeline2 = dlt.pipeline(pipeline_name="pipeline_two", destination="duckdb")
    pipeline_runner2 = PipelineRunner(pipeline2, queue=queue)
    assert pipeline_runner2.scraping_resource.name == "pipeline_two_results"


@pytest.mark.skip(
    reason=(
        "This test should run in isolation and a new interpreter"
        "for each parametrized destination"
    )
)
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@mock.patch("sources.scraping.runner.CrawlerProcess", TestCrawlerProcess)
def test_scraping_all_resources(destination_name: str, shutdown_reactor) -> None:
    pipeline_name = f"scraping_forked_{destination_name}_results"
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination_name,
        dataset_name="quotes",
    )

    run_pipeline(
        pipeline,
        MySpider,
        dataset_name="quotes",
        write_disposition="append",
    )

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = {
        pipeline_name,
        "quotes__quote__tags",
    }
    assert set(table_counts.keys()) >= set(expected_tables)

    table_expect_at_least_n_records(pipeline_name, 100, pipeline)
    table_expect_at_least_n_records("quotes__quote__tags", 232, pipeline)
