import pytest

import dlt
from sources.notion import notion_pages
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_pages(destination_name: str):
    pipeline = dlt.pipeline(
        pipeline_name="notion",
        destination=destination_name,
        dataset_name="notion_data",
        dev_mode=True,
    )

    info = pipeline.run(notion_pages())
    assert_load_info(info)

    loaded_tables = set(t["name"] for t in pipeline.default_schema.data_tables())

    expected_tables = {
        "notion_pages",
        "notion_pages__heading_1__rich_text",
        "notion_pages__bulleted_list_item__rich_text",
    }

    assert loaded_tables == expected_tables
    assert all(c > 0 for c in load_table_counts(pipeline, *expected_tables).values())


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_selected_pages(destination_name: str):
    page_id = "29298248-6067-4332-b6db-ca516d4d9af3"

    pipeline = dlt.pipeline(
        pipeline_name="notion",
        destination=destination_name,
        dataset_name="notion_data",
        dev_mode=True,
    )

    info = pipeline.run(notion_pages(page_ids=[page_id]))
    assert_load_info(info)

    loaded_tables = set(t["name"] for t in pipeline.default_schema.data_tables())
    expected_tables = {
        "notion_pages",
        "notion_pages__heading_1__rich_text",
        "notion_pages__bulleted_list_item__rich_text",
    }

    assert loaded_tables == expected_tables

    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM notion_pages") as cur:
            row = cur.fetchone()
            assert row[0] == "block"
            assert row[3] == page_id
