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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_page_stats(destination_name: str) -> None:
    pipeline = _make_pipeline(destination_name)
    data = source(site_urls=["satellite.me"])   # TODO replace with domain that the CI has access to

    info = pipeline.run(data.with_resources("page_stats"))
    assert_load_info(info)

    # assert created table schema
    schema = pipeline.default_schema
    tables = schema.data_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "bing_page_stats"

    expected_keys = [
        "date",
        "page",
        "site_url",
        "avg_click_position",
        "avg_impression_position",
        "clicks",
        "impressions",
        "_dlt_load_id",
        "_dlt_id"
    ]
    assert set(expected_keys) == set(tables[0]["columns"].keys())

    # assert data was loaded
    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT COUNT(1) FROM bing_page_stats"
        ) as cur:
            rows = list(cur.fetchall())
            assert len(rows) > 0
            assert rows[0][0] > 0
