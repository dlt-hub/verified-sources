import json
from unittest import mock

import bson
import dlt
import pyarrow
import pytest
from pendulum import DateTime, timezone
from unittest import mock

import dlt
from dlt.pipeline.exceptions import PipelineStepFailed

from sources.mongodb import mongodb, mongodb_collection
from sources.mongodb_pipeline import (
    load_entire_database,
    load_select_collection_db,
    load_select_collection_db_filtered,
    load_select_collection_db_items_parallel,
)
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mongodb",
        destination=destination_name,
        dataset_name="mongodb_data",
        dev_mode=True,
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
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_incremental(
    start, end, count1, count2, last_value_func, destination_name, data_item_format
):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
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
            data_item_format=data_item_format,
        )
    )
    for i, c in enumerate(comments[1:], start=1):
        if last_value_func is max:
            assert start <= c["date"] <= end  # check value
            assert c["date"] >= comments[i - 1]["date"]  # check order
        else:
            assert start >= c["date"] >= end  # check value
            assert c["date"] <= comments[i - 1]["date"]  # check order

    if data_item_format == "object":
        assert len(comments) == count1
    elif data_item_format == "arrow":
        assert len(comments[0]) == count1

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


@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_parallel_loading(data_item_format):
    st_records = load_select_collection_db_items_parallel(
        data_item_format, parallel=False
    )
    parallel_records = load_select_collection_db_items_parallel(
        data_item_format, parallel=True
    )
    assert len(st_records) == len(parallel_records)


@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_limit(destination_name, data_item_format):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    comments = mongodb_collection(
        collection="comments", limit=10, data_item_format=data_item_format
    )
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
        dev_mode=True,
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
        dev_mode=True,
    )
    comments = mongodb_collection(collection="comments", limit=10)

    with mock.patch("dlt.common.logger.warning") as warn_mock:
        pipeline.run(comments)

        warn_mock.assert_called_once_with(
            "Using limit without ordering - results may be inconsistent."
        )


@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_limit_chunk_size(destination_name, data_item_format):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    comments = mongodb_collection(
        collection="comments",
        limit=15,
        parallel=True,
        chunk_size=2,
        incremental=dlt.sources.incremental("date"),
        data_item_format=data_item_format,
    )
    pipeline.run(comments)

    table_counts = load_table_counts(pipeline, "comments")
    assert table_counts["comments"] == 15


@pytest.mark.parametrize("row_order", ["asc", "desc", None])
@pytest.mark.parametrize(
    "data_item_format,last_value_func",
    [
        ("object", min),
        ("object", max),
        ("object", lambda x: max(x)),
        ("arrow", min),
        ("arrow", max),
    ],
)
def test_order(row_order, last_value_func, data_item_format):
    comments = list(
        mongodb_collection(
            collection="comments",
            incremental=dlt.sources.incremental(
                "date",
                initial_value=DateTime(2005, 1, 1, tzinfo=timezone("UTC")),
                last_value_func=last_value_func,
                row_order=row_order,
            ),
            data_item_format=data_item_format,
        )
    )
    if data_item_format == "arrow":
        comments = comments[0].to_pylist()

    for i, c in enumerate(comments[1:], start=1):
        if (last_value_func is max and row_order == "asc") or (
            last_value_func is min and row_order == "desc"
        ):
            assert c["date"] >= comments[i - 1]["date"]

        if (last_value_func is min and row_order == "asc") or (
            last_value_func is max and row_order == "desc"
        ):
            assert c["date"] <= comments[i - 1]["date"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_python_types(destination_name):
    # test Python types
    res = mongodb_collection(collection="types_test")
    types = list(res)

    for field, value in {
        "field1": None,
        "field2": True,
        "field3": 1,
        "field4": bson.int64.Int64(1),
        "field5": 1.2,
        "field6": "text",
        "field7": [1, 2, 3],
        "field8": {"key": "value"},
        "field9": DateTime(2024, 1, 1, 0, 0, 0, tzinfo=timezone("UTC")),
        "field10": "^foo",
        "field11": b"foo",
        "field12": "daad12312312312312312312",
        "field13": bson.code.Code("function() { return 1; }"),
        "field14": DateTime(1970, 1, 1, 0, 0, 1, tzinfo=timezone("UTC")),
        "field15": "1.2",
        "field16": bytes("foo", "utf-8"),
    }.items():
        assert types[0][field] == value

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    info = pipeline.run(res)
    assert info.loads_ids != []


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_arrow_types(destination_name):
    # test Arrow types
    res = mongodb_collection(collection="types_test", data_item_format="arrow")
    types = list(res)[0]

    for field, value in {
        "field2": pyarrow.scalar(True),
        "field3": pyarrow.scalar(1, type=pyarrow.int32()),
        "field4": pyarrow.scalar(1),
        "field5": pyarrow.scalar(1.2),
        "field6": pyarrow.scalar("text"),
        "field7": pyarrow.scalar([1, 2, 3], type=pyarrow.list_(pyarrow.int32())),
        "field8": pyarrow.scalar({"key": "value"}),
        "field9": pyarrow.scalar(DateTime(2024, 1, 1, 0, 0, 0)),
        "field11": b"foo",
        "field12": pyarrow.scalar("daad12312312312312312312"),
        "field13": "function() { return 1; }",
        "field15": "1.2",
    }.items():
        if field in ("field11", "field13", "field15"):
            assert types[field][0].as_py() == value
            continue

        if field == "field9":
            assert types[field][0].as_py() == value.as_py()
            continue

        assert types[field][0] == value

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )

    if destination_name in ("bigquery", "postgres"):
        res = list(res)[0]
        res = res.drop_columns(["field7", "field8"])

    info = pipeline.run(res, table_name="types_test")
    assert info.loads_ids != []


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_filter(destination_name):
    """
    The field `runtime` is not set in some movies,
    thus incremental will not work. However, adding
    an explicit filter_, which says to consider
    only documents with `runtime`, makes it work.
    """
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    movies = mongodb_collection(
        collection="movies",
        incremental=dlt.sources.incremental("runtime", initial_value=500),
        filter_={"runtime": {"$exists": True}},
    )
    pipeline.run(movies)

    table_counts = load_table_counts(pipeline, "movies")
    assert table_counts["movies"] == 23


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_filter_intersect(destination_name):
    """
    Check that using in the filter_ fields that
    are used by incremental is not allowed.
    """
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    movies = mongodb_collection(
        collection="movies",
        incremental=dlt.sources.incremental("runtime", initial_value=20),
        filter_={"runtime": {"$gte": 20}},
    )

    with pytest.raises(PipelineStepFailed):
        pipeline.run(movies)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_projection_list_inclusion(destination_name):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    collection_name = "movies"
    projection = ["title", "poster"]
    expected_columns = projection + ["_id", "_dlt_id", "_dlt_load_id"]

    movies = mongodb_collection(
        collection=collection_name, projection=projection, limit=2
    )
    pipeline.run(movies)
    loaded_columns = pipeline.default_schema.get_table_columns(collection_name).keys()

    assert set(loaded_columns) == set(expected_columns)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_projection_dict_inclusion(destination_name):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    collection_name = "movies"
    projection = {"title": 1, "poster": 1}
    expected_columns = list(projection.keys()) + ["_id", "_dlt_id", "_dlt_load_id"]

    movies = mongodb_collection(
        collection=collection_name, projection=projection, limit=2
    )
    pipeline.run(movies)
    loaded_columns = pipeline.default_schema.get_table_columns(collection_name).keys()

    assert set(loaded_columns) == set(expected_columns)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_projection_dict_exclusion(destination_name):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    collection_name = "movies"
    columns_to_exclude = [
        "runtime",
        "released",
        "year",
        "plot",
        "fullplot",
        "lastupdated",
        "type",
        "directors",
        "imdb",
        "cast",
        "countries",
        "genres",
        "tomatoes",
        "num_mflix_comments",
        "rated",
        "awards",
    ]
    projection = {col: 0 for col in columns_to_exclude}
    expected_columns = ["title", "poster", "_id", "_dlt_id", "_dlt_load_id"]

    movies = mongodb_collection(
        collection=collection_name, projection=projection, limit=2
    )
    pipeline.run(movies)
    loaded_columns = pipeline.default_schema.get_table_columns(collection_name).keys()

    assert set(loaded_columns) == set(expected_columns)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_projection_nested_field(destination_name):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    collection_name = "movies"
    projection = ["imdb.votes", "poster"]
    expected_columns = ["imdb__votes", "poster", "_id", "_dlt_id", "_dlt_load_id"]
    # other documents nested under `imdb` shouldn't be loaded
    not_expected_columns = ["imdb__rating", "imdb__id"]

    movies = mongodb_collection(
        collection=collection_name, projection=projection, limit=2
    )
    pipeline.run(movies)
    loaded_columns = pipeline.default_schema.get_table_columns(collection_name).keys()

    assert set(loaded_columns) == set(expected_columns)
    assert len(set(loaded_columns).intersection(not_expected_columns)) == 0


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_mongodb_without_pymongoarrow(
    destination_name: str, data_item_format: str
) -> None:
    with mock.patch.dict("sys.modules", {"pymongoarrow": None}):
        pipeline = dlt.pipeline(
            pipeline_name="test_mongodb_without_pymongoarrow",
            destination=destination_name,
            dataset_name="test_mongodb_without_pymongoarrow_data",
            dev_mode=True,
        )

        comments = mongodb_collection(
            collection="comments", limit=10, data_item_format=data_item_format
        )
        load_info = pipeline.run(comments)

        assert load_info.loads_ids != []
        table_counts = load_table_counts(pipeline, "comments")
        assert table_counts["comments"] == 10


@pytest.mark.parametrize("convert_to_string", [True, False])
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_pymongoarrow_schema(convert_to_string: bool, destination_name: str):
    """Tests that the provided schema is correctly applied."""
    from pymongoarrow.api import Schema
    from pymongoarrow.types import BinaryType

    expected_schema = {
        "_id": bson.objectid.ObjectId,
        "field2": pyarrow.bool_(),
        "field3": pyarrow.int32(),
        "field4": pyarrow.int64(),
        "field5": pyarrow.float64(),
        "field6": pyarrow.string(),
        "field7": pyarrow.list_(pyarrow.int32()),
        "field8": pyarrow.struct({"key": pyarrow.string()}),
        "field9": pyarrow.timestamp("ms"),
        "field10": pyarrow.string(),
        "field12": pyarrow.string(),
        "field13": pyarrow.string(),
        "field14": pyarrow.timestamp("ms"),
        "field15": pyarrow.string(),
        "field11": pyarrow.string()
        if convert_to_string
        else BinaryType(subtype=0),  # Generic binary subtype
        "field16": pyarrow.string()
        if convert_to_string
        else BinaryType(subtype=0),  # Generic binary subtype
    }

    res = mongodb_collection(
        collection="types_test",
        data_item_format="arrow",
        pymongoarrow_schema=Schema(expected_schema),
    )

    actual_schema = list(res)[0].schema

    for field, expected_type in expected_schema.items():
        actual_type = actual_schema.field(field).type
        if field == "_id":
            # ObjectId is converted to a hex string
            assert actual_type == pyarrow.string()
        elif isinstance(expected_type, BinaryType):
            # Binary fields are specifically converted to pyarrow binary
            assert actual_type == pyarrow.binary()
        else:
            assert actual_type == expected_type

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination=destination_name,
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )

    if destination_name in ("bigquery", "postgres"):
        res = list(res)[0]
        res = res.drop_columns(["field7", "field8"])

    info = pipeline.run(res, table_name="types_test")
    assert info.loads_ids != []
