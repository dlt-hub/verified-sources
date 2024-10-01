import pytest

from sources.notion import notion_databases

import dlt

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_notion_databases(destination_name: str):
    pipeline = dlt.pipeline(
        pipeline_name="notion",
        destination=destination_name,
        dataset_name="notion_data",
        dev_mode=True,
    )

    info = pipeline.run(notion_databases())
    assert_load_info(info)

    expected_tables = {
        "sales_crm",
        "sales_crm__properties__account_owner__people",
        "sales_crm__properties__company__rich_text",
        "sales_crm__properties__name__title",
        "second_db",
        "second_db__properties__name__title",
        "second_db__properties__text_property__rich_text",
        "second_db__properties__second_db_related__relation",
        "second_db_items",
        "second_db_items__properties__text_property__rich_text",
        "second_db_items__properties__name__title",
        "second_db_items__properties__second_db__relation",
    }

    loaded_tables = set(t["name"] for t in pipeline.default_schema.data_tables())

    assert loaded_tables == expected_tables
    assert all(c > 0 for c in load_table_counts(pipeline, *expected_tables).values())


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_selected_notion_database(destination_name: str):
    sales_database = notion_databases(
        database_ids=[
            {
                "id": "a94223535c674d33a24e313e7921ce15",
                "use_name": "new_name",  # To test if use_name is used
            }
        ],
    )

    pipeline = dlt.pipeline(
        pipeline_name="notion",
        destination=destination_name,
        dataset_name="notion_data",
        dev_mode=True,
    )

    info = pipeline.run(sales_database)
    assert_load_info(info)

    expected_table_names = {
        "new_name",
        "new_name__properties__name__title",
        "new_name__properties__second_db_related__relation",
        "new_name__properties__text_property__rich_text",
    }

    data_tables = pipeline.default_schema.data_tables()

    loaded_table_names = set(t["name"] for t in data_tables)
    assert loaded_table_names == expected_table_names
    assert all(
        c > 0 for c in load_table_counts(pipeline, *expected_table_names).values()
    )

    new_name_table = next(t for t in data_tables if t["name"] == "new_name")

    # Validate data types of columns
    assert new_name_table["columns"]["created_time"]["data_type"] == "timestamp"
    assert (
        new_name_table["columns"]["properties__number_property__number"]["data_type"]
        == "bigint"
    )
    assert (
        new_name_table["columns"]["properties__number_with_commas__number"]["data_type"]
        == "double"
    )
