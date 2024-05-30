import json
from unittest import mock

import dlt
import pytest
from pendulum import DateTime, timezone

from sources.mongodb import mongodb, mongodb_collection
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


@pytest.mark.parametrize(
    "start,end,count1,count2,last_value_func",
    [
        (
            DateTime(2005, 1, 1, tzinfo=timezone("UTC")),
            DateTime(2005, 12, 31, tzinfo=timezone("UTC")),
            1119,  # [start: end] range
            12293,  # [end:] range
            max,  # asc
        ),
        (
            DateTime(2005, 6, 1, tzinfo=timezone("UTC")),
            DateTime(2005, 1, 1, tzinfo=timezone("UTC")),
            442,  # [start: end] range
            36892,  # [end:] range
            min,  # desc
        ),
    ],
)
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental(start, end, count1, count2, last_value_func, destination_name):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        full_refresh=True,
    )

    # read a particular range
    comments = list(
        mongodb_collection(
            collection="comments",
            incremental=dlt.sources.incremental(
                "date",
                initial_value=start,
                end_value=end,
                last_value_func=last_value_func,
                row_order="asc",
            ),
        )
    )
    for i, c in enumerate(comments[1:], start=1):
        if last_value_func is max:
            assert start <= c["date"] <= end  # check value
            assert c["date"] >= comments[i - 1]["date"]  # check order
        else:
            assert start >= c["date"] >= end  # check value
            assert c["date"] <= comments[i - 1]["date"]  # check order

    assert len(comments) == count1

    # read after the first range, but without end value
    comments = mongodb_collection(
        collection="comments",
        incremental=dlt.sources.incremental(
            "date", initial_value=end, last_value_func=last_value_func
        ),
    )
    for c in comments:
        if last_value_func is max:
            assert c["date"] >= end
        else:
            assert c["date"] <= end

    load_info = pipeline.run(comments)

    table_counts = load_table_counts(pipeline, "comments")
    assert load_info.loads_ids != []
    assert table_counts["comments"] == count2

    # subsequent calls must not load any data
    load_info = pipeline.run(comments)
    assert load_info.loads_ids == []


def test_parallel_loading():
    st_records = load_select_collection_db_items(parallel=False)
    parallel_records = load_select_collection_db_items(parallel=True)
    assert len(st_records) == len(parallel_records)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_limit(destination_name):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        full_refresh=True,
    )
    comments = mongodb_collection(collection="comments", limit=10)
    pipeline.run(comments)

    table_counts = load_table_counts(pipeline, "comments")
    assert table_counts["comments"] == 10


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_limit_on_source(destination_name):
    collections = ("comments", "movies")
    limit = 8

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        full_refresh=True,
    )
    comments = mongodb(collection_names=collections, limit=limit)
    pipeline.run(comments)

    table_counts = load_table_counts(pipeline, *collections)
    for col_name in collections:
        assert table_counts[col_name] == limit


@pytest.mark.parametrize("destination", ALL_DESTINATIONS)
def test_limit_warning(destination):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination,
        dataset_name="mongodb_test_data",
        full_refresh=True,
    )
    comments = mongodb_collection(collection="comments", limit=10)

    with mock.patch("dlt.common.logger.warning") as warn_mock:
        pipeline.run(comments)

        warn_mock.assert_called_once_with(
            "Using limit without ordering - results may be inconsistent."
        )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_limit_chunk_size(destination_name):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        full_refresh=True,
    )
    comments = mongodb_collection(
        collection="comments",
        limit=15,
        parallel=True,
        chunk_size=2,
        incremental=dlt.sources.incremental("date"),
    )
    pipeline.run(comments)

    table_counts = load_table_counts(pipeline, "comments")
    assert table_counts["comments"] == 15


@pytest.mark.parametrize("row_order", ["asc", "desc", None])
@pytest.mark.parametrize("last_value_func", [min, max, lambda x: max(x)])
def test_order(row_order, last_value_func):
    comments = list(
        mongodb_collection(
            collection="comments",
            incremental=dlt.sources.incremental(
                "date",
                initial_value=DateTime(2005, 1, 1, tzinfo=timezone("UTC")),
                last_value_func=last_value_func,
                row_order=row_order,
            ),
        )
    )
    for i, c in enumerate(comments[1:], start=1):
        if (last_value_func is max and row_order == "asc") or (
            last_value_func is min and row_order == "desc"
        ):
            assert c["date"] >= comments[i - 1]["date"]

        if (last_value_func is min and row_order == "asc") or (
            last_value_func is max and row_order == "desc"
        ):
            assert c["date"] <= comments[i - 1]["date"]
