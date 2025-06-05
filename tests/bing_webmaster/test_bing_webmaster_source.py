import dlt
import pytest

from sources.bing_webmaster import source
from tests.utils import ALL_DESTINATIONS, assert_load_info


def _make_pipeline(destination_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="bing_webmaster_test",
        destination=destination_name,
        dataset_name="bing_webmaster_test_data",
        dev_mode=True,
    )


@pytest.mark.parametrize("destination_name", ["duckdb"])
def test_load_unauthorized_domain(destination_name: str) -> None:
    pipeline = _make_pipeline(destination_name)
    data = source(site_urls=["wikipedia.org"], site_url_pages=[])
    with pytest.raises(Exception):
        pipeline.run(data.with_resources("page_stats"))


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_page_stats(destination_name: str) -> None:
    pipeline = _make_pipeline(destination_name)
    table_name = "bing_page_stats"

    # Note: If this test fails: replace this with a site_url you can access
    data = source(site_urls=["dlthub.com"], site_url_pages=[])

    info = pipeline.run(data.with_resources("page_stats"))
    assert_load_info(info)

    # assert created table schema
    tables = pipeline.default_schema.data_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == table_name
    expected_keys = [
        "date",
        "site_url",
        "page",
        "clicks",
        "impressions",
        "avg_click_position",
        "avg_impression_position",
        "_dlt_load_id",
        "_dlt_id",
    ]
    assert set(expected_keys) == set(tables[0]["columns"].keys())

    # assert data was loaded
    with pipeline.sql_client() as c:
        with c.execute_query(f"SELECT COUNT(1) FROM {table_name}") as cur:
            row_count = cur.fetchall()
            assert row_count[0][0] > 0


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_page_query_stats(destination_name: str) -> None:
    pipeline = _make_pipeline(destination_name)
    table_name = "bing_page_query_stats"

    data = source(
        site_urls=["dlthub.com"],
        # Note: If this test fails: replace this with site_url and pages you can access
        site_url_pages=[
            {"site_url": "dlthub.com", "page": "https://dlthub.com/docs/intro"},
            {"site_url": "dlthub.com", "page": "https://dlthub.com/why/"},
        ],
    )
    info = pipeline.run(data.with_resources("page_query_stats"))
    assert_load_info(info)

    # assert created table schema
    tables = pipeline.default_schema.data_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == table_name

    expected_keys = [
        "date",
        "site_url",
        "page",
        "query",
        "clicks",
        "impressions",
        "avg_click_position",
        "avg_impression_position",
        "_dlt_load_id",
        "_dlt_id",
    ]
    assert set(expected_keys) == set(tables[0]["columns"].keys())

    # assert data was loaded
    with pipeline.sql_client() as c:
        with c.execute_query(f"SELECT COUNT(1) FROM {table_name}") as cur:
            row_count = cur.fetchall()
            assert row_count[0][0] > 0
