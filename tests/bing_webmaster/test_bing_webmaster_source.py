import pytest

import dlt

from sources.bing_webmaster import source, page_stats, page_query_stats

from tests.utils import ALL_DESTINATIONS, assert_load_info


def _make_pipeline(destination_name: str) -> dlt.pipeline:
    return dlt.pipeline(
        pipeline_name="bing_webmaster_test",
        destination=destination_name,
        dataset_name="bing_webmaster_test_data",
        full_refresh=True,
    )


@pytest.mark.parametrize("destination_name", ["duckdb"])
def test_load_unauthorized_domain(destination_name: str) -> None:
    pipeline = _make_pipeline(destination_name)
    data = source(site_urls=["wikipedia.org"])
    with pytest.raises(Exception):
        pipeline.run(data.with_resources("page_stats"))
