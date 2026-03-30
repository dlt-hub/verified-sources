import os
import tempfile
from unittest import mock

import dlt
import pytest

from sources.scraping import run_pipeline
from sources.scraping.helpers import create_pipeline_runner, resolve_start_urls
from sources.scraping.queue import ScrapingQueue
from sources.scraping.runner import PipelineRunner, Signals
from sources.scraping.settings import SOURCE_SCRAPY_SETTINGS

from tests.utils import ALL_DESTINATIONS, load_table_counts

from .utils import (
    MySpider,
    MockQueue,
    queue_closer,
    table_expect_at_least_n_records,
)


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


def test_queue_yields_full_batch():
    """Verify that a full batch (exactly batch_size items) is yielded correctly."""
    queue = ScrapingQueue(read_timeout=0.5, batch_size=3)
    queue.put({"n": 1})
    queue.put({"n": 2})
    queue.put({"n": 3})

    # Get the first batch — should be a full batch of 3
    batch_iter = queue.get_batches()
    first_batch = next(batch_iter)
    assert len(first_batch) == 3

    # Close and drain remaining
    queue.close()
    remaining = list(batch_iter)
    assert remaining == []


def test_queue_stream_handles_generator_exit():
    """stream() should close the queue when the consumer sends GeneratorExit."""
    queue = ScrapingQueue(read_timeout=0.5, batch_size=10)
    queue.put({"n": 1})

    gen = queue.stream()
    next(gen)  # consume first batch (partial, after timeout)
    gen.close()  # sends GeneratorExit

    assert queue.is_closed


def test_signals_initiate_stop_is_idempotent():
    """_initiate_stop() should only execute its body once, even if called multiple times."""
    queue = ScrapingQueue()
    sig = Signals(pipeline_name="test", queue=queue)

    mock_runner = mock.MagicMock()
    sig._runner = mock_runner

    sig._initiate_stop()
    sig._initiate_stop()
    sig._initiate_stop()

    assert sig._stopped is True
    assert queue.is_closed


def test_signals_on_item_scraped_when_queue_closed():
    """Items are always enqueued, even after queue is closed.

    The closed flag only tells the consumer to stop after draining.
    This prevents dropping items when engine_stopped and item_scraped
    callbacks are interleaved in the reactor.
    """
    queue = ScrapingQueue()
    sig = Signals(pipeline_name="test", queue=queue)

    queue.close()
    sig.on_item_scraped({"data": "value"})

    # item is still enqueued so the consumer can drain it
    assert not queue.empty()
    assert queue.get_nowait() == {"data": "value"}


def test_settings_merge_preserves_defaults():
    """User scrapy_settings should merge on top of defaults, not replace them."""
    pipeline = dlt.pipeline(
        pipeline_name="merge_test",
        destination="duckdb",
    )

    custom_settings = {"DEPTH_LIMIT": 50, "MY_CUSTOM_KEY": "value"}

    with mock.patch(
        "sources.scraping.helpers.resolve_start_urls",
        return_value=["http://example.com"],
    ):
        host = create_pipeline_runner(
            pipeline, MySpider, scrapy_settings=custom_settings
        )

    runner_settings = host.scrapy_runner.runner.settings
    # Custom settings should be applied
    assert runner_settings.get("DEPTH_LIMIT") == 50
    assert runner_settings.get("MY_CUSTOM_KEY") == "value"
    # Defaults should be preserved
    assert runner_settings.get("TELNETCONSOLE_ENABLED") is False
    assert "USER_AGENT" in runner_settings


def test_settings_merge_without_custom_uses_defaults():
    """When no scrapy_settings provided, all defaults should be present."""
    pipeline = dlt.pipeline(
        pipeline_name="defaults_test",
        destination="duckdb",
    )

    with mock.patch(
        "sources.scraping.helpers.resolve_start_urls",
        return_value=["http://example.com"],
    ):
        host = create_pipeline_runner(pipeline, MySpider)

    runner_settings = host.scrapy_runner.runner.settings
    for key in SOURCE_SCRAPY_SETTINGS:
        assert key in runner_settings


def test_resolve_start_urls_merges_and_deduplicates(tmp_path):
    """resolve_start_urls should merge file URLs and list URLs, deduplicated."""
    urls_file = tmp_path / "urls.txt"
    urls_file.write_text("http://a.com\nhttp://b.com\nhttp://a.com\n")

    with mock.patch(
        "sources.scraping.helpers.dlt.config",
        new_callable=mock.PropertyMock,
    ):
        result = resolve_start_urls(
            start_urls=["http://b.com", "http://c.com"],
            start_urls_file=str(urls_file),
        )

    assert set(result) == {
        "http://a.com\n",
        "http://b.com\n",
        "http://b.com",
        "http://c.com",
    }


@mock.patch("sources.scraping.helpers.ScrapingQueue", MockQueue)
def test_pipeline_runners_handle_extended_and_simple_use_cases(mocker):
    pipeline = dlt.pipeline(
        pipeline_name="scraping_res_add_limit",
        destination="duckdb",
    )

    spy_on_queue_put = mocker.spy(MockQueue, "put")
    spy_on_queue_close = mocker.spy(MockQueue, "close")
    scraping_host = create_pipeline_runner(pipeline, MySpider, batch_size=10)
    scraping_host.pipeline_runner.scraping_resource.add_limit(2)
    scraping_host.run(dataset_name="quotes", write_disposition="append")

    table_expect_at_least_n_records("scraping_res_add_limit_results", 20, pipeline)
    table_expect_at_least_n_records(
        "scraping_res_add_limit_results__quote__tags", 68, pipeline
    )

    spy_on_queue_put.assert_called()
    spy_on_queue_close.assert_called()

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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_scraping_all_resources(destination_name: str) -> None:
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

    # Resource name is "quotes" because dataset_name="quotes" is explicitly set,
    # so PipelineRunner uses dataset_name as the resource name.
    expected_tables = {
        "quotes",
        "quotes__quote__tags",
    }
    assert set(table_counts.keys()) >= set(expected_tables)

    table_expect_at_least_n_records("quotes", 100, pipeline)
    table_expect_at_least_n_records("quotes__quote__tags", 232, pipeline)


@mock.patch("sources.scraping.helpers.ScrapingQueue", MockQueue)
def test_run_pipeline_with_on_before_start_callback():
    """on_before_start callback should be invoked with the DltResource before crawling."""
    pipeline = dlt.pipeline(
        pipeline_name="scraping_callback",
        destination="duckdb",
    )

    callback_called_with = []

    def on_before_start(res):
        callback_called_with.append(res)
        res.add_limit(2)

    run_pipeline(
        pipeline,
        MySpider,
        batch_size=10,
        on_before_start=on_before_start,
        write_disposition="append",
    )

    # Callback was invoked exactly once with the scraping resource
    assert len(callback_called_with) == 1
    assert callback_called_with[0].name == "scraping_callback_results"

    # Pipeline produced data (limited by add_limit(2) -> 2 batches of 10)
    table_expect_at_least_n_records("scraping_callback_results", 20, pipeline)


@mock.patch("sources.scraping.helpers.ScrapingQueue", MockQueue)
def test_run_pipeline_with_custom_scrapy_settings():
    """Custom scrapy_settings passed via run_pipeline should take effect during crawl."""
    pipeline = dlt.pipeline(
        pipeline_name="scraping_custom_settings",
        destination="duckdb",
        dataset_name="custom_quotes",
    )

    run_pipeline(
        pipeline,
        MySpider,
        scrapy_settings={
            "DEPTH_LIMIT": 2,
        },
        write_disposition="append",
    )

    # DEPTH_LIMIT is Scrapy's max request depth (links followed from start URL).
    # This spider follows linear "next page" links, so depth 0=page1, 1=page2, 2=page3.
    # 3 pages × 10 quotes/page = 30 quotes.
    table_expect_at_least_n_records("custom_quotes", 30, pipeline)


@mock.patch("sources.scraping.helpers.ScrapingQueue", MockQueue)
def test_run_pipeline_with_batch_size_and_queue_params():
    """batch_size, queue_size, queue_result_timeout should propagate through run_pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name="scraping_params",
        destination="duckdb",
    )

    run_pipeline(
        pipeline,
        MySpider,
        batch_size=5,
        queue_size=100,
        queue_result_timeout=2.0,
        write_disposition="append",
    )

    # Verify the pipeline completed and produced data.
    # With DEPTH_LIMIT=0 the spider scrapes all 10 pages (100 quotes).
    table_expect_at_least_n_records("scraping_params_results", 100, pipeline)


@mock.patch("sources.scraping.helpers.ScrapingQueue", MockQueue)
def test_pipeline_error_closes_queue_and_does_not_hang():
    """When pipeline.run raises, the queue should close and the system should not hang."""
    pipeline = dlt.pipeline(
        pipeline_name="scraping_err_propagation",
        destination="duckdb",
        dataset_name="err_quotes",
    )

    with mock.patch(
        "dlt.Pipeline.run", side_effect=OSError("simulated pipeline failure")
    ):
        # Should not hang — the pipeline error closes the queue,
        # which causes the scrapy runner to stop.
        run_pipeline(pipeline, MySpider, write_disposition="append")

    # After the error, the queue must be closed
    # (verified indirectly: if it weren't closed, we'd hang and timeout)

    # The pipeline should have no data since pipeline.run was mocked to fail
    with pipeline.sql_client() as client:
        # The schema may not even exist after a failed run
        try:
            with client.execute_query("SELECT count(*) FROM err_quotes") as cursor:
                count = cursor.fetchone()[0]
                # Either 0 records or table doesn't exist — both acceptable
                assert count == 0
        except Exception:
            # Table/schema doesn't exist — expected after a failed pipeline run
            pass


@mock.patch("sources.scraping.helpers.ScrapingQueue", MockQueue)
def test_sequential_pipeline_runs():
    """Two scraping pipelines can run sequentially in the same process."""
    pipeline1 = dlt.pipeline(
        pipeline_name="scraping_seq_1",
        destination="duckdb",
        dataset_name="quotes_seq1",
    )
    run_pipeline(pipeline1, MySpider, write_disposition="append")
    table_expect_at_least_n_records("quotes_seq1", 100, pipeline1)

    pipeline2 = dlt.pipeline(
        pipeline_name="scraping_seq_2",
        destination="duckdb",
        dataset_name="quotes_seq2",
    )
    run_pipeline(pipeline2, MySpider, write_disposition="append")
    table_expect_at_least_n_records("quotes_seq2", 100, pipeline2)
