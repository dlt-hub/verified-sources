import json

import dlt
import pytest
from pendulum import DateTime, Timezone

from sources.mongodb import mongodb_collection
from sources.mongodb_pipeline import (
    load_entire_database,
    load_select_collection_db,
    load_select_collection_db_filtered,
    load_select_collection_db_items,
)
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mongodb",
        destination=destination_name,
        dataset_name="mongodb_data",
        full_refresh=True,
    )

    # Load entire database
    load_info = load_entire_database(pipeline)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    expected_tables = ["comments", "movies", "sessions"]
    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["sessions"] == 1

    # Load selected collection with different settings
    load_info = load_select_collection_db(pipeline)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    expected_tables = ["comments"]
    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["comments"] == 50304

    # Load collection with filter
    load_info = load_select_collection_db_filtered(pipeline)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    expected_tables = ["movies"]
    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["movies"] == 23539


def test_nested_documents():
    movies = mongodb_collection(collection="movieGroups")
    document = list(movies)[0]
    # Check the date type
    assert isinstance(document["date"], DateTime)
    # All other fields must be json serializable
    del document["date"]
    doc_str = json.dumps(document)
    # Confirm that we are using the right object with nested fields
    assert json.loads(doc_str)["_id"] == "651c075367e4e330ec801dac"


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_order(order):
    comments = list(
        mongodb_collection(
            collection="comments",
            incremental=dlt.sources.incremental("date", row_order=order),
        ),
    )

    for i, c in enumerate(comments[1:], start=1):
        if order == "desc":
            assert c["date"] <= comments[i - 1]["date"]
        else:
            assert c["date"] >= comments[i - 1]["date"]

    assert len(comments) == 50304


def test_start_end():
    start = DateTime(2005, 1, 1, tzinfo=Timezone("UTC"))
    end = DateTime(2005, 12, 31, tzinfo=Timezone("UTC"))

    comments = list(
        mongodb_collection(
            collection="comments",
            incremental=dlt.sources.incremental(
                "date", initial_value=start, end_value=end
            ),
        ),
    )
    for c in comments:
        assert start <= c["date"] <= end

    assert len(comments) == 1119


def test_parallel_loading():
    st_records = load_select_collection_db_items(parallel=False)
    parallel_records = load_select_collection_db_items(parallel=True)
    assert len(st_records) == len(parallel_records)
