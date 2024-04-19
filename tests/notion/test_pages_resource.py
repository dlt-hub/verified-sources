import pytest

import dlt
from sources.notion import notion_pages
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_notion_databases(destination_name: str):
    pipeline = dlt.pipeline(
        pipeline_name="notion",
        destination=destination_name,
        dataset_name="notion_data",
        full_refresh=True,
    )

    info = pipeline.run(notion_pages())
    assert_load_info(info)

    loaded_tables = set(t["name"] for t in pipeline.default_schema.data_tables())

    expected_tables = {
        "notion_pages__properties__second_db_related__relation",
        "notion_pages__properties__second_db__relation",
        "notion_pages__properties__account_owner__people",
        "notion_pages__properties__title__title",
        "notion_pages",
        "notion_pages__properties__text_property__rich_text",
        "notion_pages__properties__name__title",
        "notion_pages__properties__company__rich_text",
    }

    assert loaded_tables == expected_tables
    assert all(c > 0 for c in load_table_counts(pipeline, *expected_tables).values())
