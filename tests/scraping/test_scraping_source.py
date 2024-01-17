from queue import Queue
from typing import Any, Callable, Dict, Generator, Iterable, Optional
import dlt
import pytest
from dlt.sources import DltResource
from scrapy.http import Response  # type: ignore

from sources.scraping import build_scrapy_source
from sources.scraping.helpers import start_pipeline
from tests.utils import ALL_DESTINATIONS, load_table_counts

start_urls = ["https://quotes.toscrape.com/page/1/"]


def parse(response: Response) -> Generator[Dict[str, Any], None, None]:
    for quote in response.css("div.quote"):
        yield {
            "quote": {
                "text": quote.css("span.text::text").get(),
                "author": quote.css("small.author::text").get(),
                "tags": quote.css("div.tags a.tag::text").getall(),
            },
        }


def next_page(response: Response) -> Optional[str]:
    return str(response.css("li.next a::attr(href)").get())


def pipeline_runner(
    pipeline: dlt.Pipeline,
    source: Iterable[DltResource],
) -> Callable[[], None]:
    def run() -> None:
        load_info = pipeline.run(
            source,
            table_name="famous_quotes",
            write_disposition="replace",
        )
        print(load_info)

    return run


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="famous_quotes",
        destination=destination_name,
        dataset_name="quotes",
        full_refresh=True,
    )

    scrapy_runner, scrapy_source = build_scrapy_source(
        on_result=parse,
        on_next_page=next_page,
        start_urls=start_urls,
    )

    start_pipeline(
        pipeline_runner(pipeline, scrapy_source()),
        scrapy_runner,
    )

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = {"fam_quotes", "fam_quotes__quote__tags"}
    assert set(table_counts.keys()) >= set(expected_tables)

    assert table_counts["fam_quotes"] == 100
    assert table_counts["fam_quotes__quote__tags"] == 232
