import json
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest import mock

import bson
import dlt
import pyarrow
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources import DltResource
from pymongo.database import Database

from sources.mongodb import mongodb, mongodb_collection
from sources.mongodb_pipeline import (
    load_entire_database,
    load_select_collection_db,
    load_select_collection_db_filtered,
    load_select_collection_db_items_parallel,
)
from tests.mongodb.conftest import LOCAL_MONGO_URL
from tests.mongodb.utils import make_documents
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    skip_if_not_active,
    skipifwindows,
)


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


@skip_if_not_active("duckdb")
def test_nested_documents():
    movies = mongodb_collection(collection="movieGroups")
    document = list(movies)[0]
    # Check the date type
    assert isinstance(document["date"], datetime)
    assert document["date"].tzinfo == timezone.utc
    # All other fields must be json serializable
    del document["date"]
    doc_str = json.dumps(document)
    # Confirm that we are using the right object with nested fields
    assert json.loads(doc_str)["_id"] == "651c075367e4e330ec801dac"


@pytest.mark.parametrize(
    "start,end,count1,count2,last_value_func",
    [
        (
            datetime(2005, 1, 1, tzinfo=timezone.utc),
            datetime(2005, 12, 31, tzinfo=timezone.utc),
            1119,  # [start: end] range
            12293,  # [end:] range
            max,  # asc
        ),
        (
            datetime(2005, 6, 1, tzinfo=timezone.utc),
            datetime(2005, 1, 1, tzinfo=timezone.utc),
            442,  # [start: end] range
            36892,  # [end:] range
            min,  # desc
        ),
    ],
)
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_incremental(start, end, count1, count2, last_value_func, data_item_format):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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


def _count_rows(items) -> int:
    return sum(
        item.num_rows if isinstance(item, pyarrow.Table) else 1 for item in items
    )


@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_parallel_loading(data_item_format):
    st_records = load_select_collection_db_items_parallel(
        data_item_format, parallel=False
    )
    parallel_records = load_select_collection_db_items_parallel(
        data_item_format, parallel=True
    )
    assert _count_rows(st_records) == _count_rows(parallel_records) > 0


@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@skip_if_not_active("duckdb")
def test_limit(data_item_format):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    comments = mongodb_collection(
        collection="comments", limit=10, data_item_format=data_item_format
    )
    pipeline.run(comments)

    table_counts = load_table_counts(pipeline, "comments")
    assert table_counts["comments"] == 10


@skip_if_not_active("duckdb")
def test_limit_on_source():
    collections = ("comments", "movies")
    limit = 8

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
        dataset_name="mongodb_test_data",
        dev_mode=True,
    )
    comments = mongodb(collection_names=collections, limit=limit)
    pipeline.run(comments)

    table_counts = load_table_counts(pipeline, *collections)
    for col_name in collections:
        assert table_counts[col_name] == limit


@skip_if_not_active("duckdb")
def test_limit_warning():
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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
@skip_if_not_active("duckdb")
def test_limit_chunk_size(data_item_format):
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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


@skip_if_not_active("duckdb")
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
                initial_value=datetime(2005, 1, 1, tzinfo=timezone.utc),
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
        "field9": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "field10": "^foo",
        "field11": b"foo",
        "field12": "daad12312312312312312312",
        "field13": bson.code.Code("function() { return 1; }"),
        "field14": datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
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
        # nested documents are flattened like in the object format
        "field8__key": pyarrow.scalar("value"),
        "field9": pyarrow.scalar(datetime(2024, 1, 1, tzinfo=timezone.utc)),
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
        res = res.drop_columns(["field7"])

    info = pipeline.run(res, table_name="types_test")
    assert info.loads_ids != []


@skip_if_not_active("duckdb")
def test_filter():
    """
    The field `runtime` is not set in some movies,
    thus incremental will not work. However, adding
    an explicit filter_, which says to consider
    only documents with `runtime`, makes it work.
    """
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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


@skip_if_not_active("duckdb")
def test_filter_intersect():
    """
    Check that using in the filter_ fields that
    are used by incremental is not allowed.
    """
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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


@skip_if_not_active("duckdb")
def test_projection_list_inclusion():
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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


@skip_if_not_active("duckdb")
def test_projection_dict_inclusion():
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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


@skip_if_not_active("duckdb")
def test_projection_dict_exclusion():
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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


@skip_if_not_active("duckdb")
def test_projection_nested_field():
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_test",
        destination="duckdb",
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


@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_mongodb_without_pymongoarrow(data_item_format: str) -> None:
    with mock.patch.dict("sys.modules", {"pymongoarrow": None}):
        pipeline = dlt.pipeline(
            pipeline_name="test_mongodb_without_pymongoarrow",
            destination="duckdb",
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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_pymongoarrow_schema(destination_name: str):
    """Tests that the provided schema is correctly applied."""
    from pymongoarrow.api import Schema
    from pymongoarrow.types import BinaryType, CodeType

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
        "field13": CodeType(),
        "field14": pyarrow.timestamp("ms"),
        "field15": pyarrow.string(),
        "field11": BinaryType(subtype=0),
        "field16": BinaryType(subtype=0),
    }

    res = mongodb_collection(
        collection="types_test",
        data_item_format="arrow",
        pymongoarrow_schema=Schema(expected_schema),
    )

    actual_schema = list(res)[0].schema

    for field, expected_type in expected_schema.items():
        if isinstance(expected_type, pyarrow.StructType):
            # nested documents are flattened like in the object format
            for child in expected_type:
                actual_type = actual_schema.field(f"{field}__{child.name}").type
                assert actual_type == child.type
            continue
        actual_type = actual_schema.field(field).type
        if field == "_id":
            # ObjectId is converted to a hex string
            assert actual_type == pyarrow.string()
        elif isinstance(expected_type, CodeType):
            # Code fields are converted to their string storage
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
        res = res.drop_columns(["field7"])

    info = pipeline.run(res, table_name="types_test")
    assert info.loads_ids != []


def _local_rows(pipeline: dlt.Pipeline, table_name: str) -> List[Dict[str, Any]]:
    columns = ["_id", "n", "title", "updated_at", "price", "payload", "code"]
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            f"SELECT {', '.join(columns)} FROM {table_name} ORDER BY n"
        )
    return [dict(zip(columns, row)) for row in rows]


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("parallel", [False, True], ids=["sequential", "parallel"])
def test_local_arrow_matches_object(local_mongo: Database, parallel: bool) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_local_formats", destination="duckdb", dev_mode=True
    )
    for data_item_format in ("object", "arrow"):
        docs = mongodb_collection(
            connection_url=LOCAL_MONGO_URL,
            database=local_mongo.name,
            collection="docs",
            parallel=parallel,
            chunk_size=4,
            data_item_format=data_item_format,
        )
        pipeline.run(docs, table_name=f"docs_{data_item_format}")

    object_rows = _local_rows(pipeline, "docs_object")
    assert len(object_rows) == 25
    assert len({row["_id"] for row in object_rows}) == 25
    assert _local_rows(pipeline, "docs_arrow") == object_rows


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize("parallel", [False, True], ids=["sequential", "parallel"])
def test_local_incremental(
    local_mongo: Database, data_item_format: str, parallel: bool
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_local_incremental", destination="duckdb", dev_mode=True
    )

    def docs() -> DltResource:
        return mongodb_collection(
            connection_url=LOCAL_MONGO_URL,
            database=local_mongo.name,
            collection="docs",
            incremental=dlt.sources.incremental(
                "updated_at",
                initial_value=datetime(2024, 1, 11, tzinfo=timezone.utc),
                row_order="asc",
            ),
            projection=["title"],
            parallel=parallel,
            chunk_size=4,
            data_item_format=data_item_format,
        )

    pipeline.run(docs())
    assert load_table_counts(pipeline, "docs") == {"docs": 15}
    # the projection keeps the cursor field
    assert {"_id", "title", "updated_at"} <= set(
        pipeline.default_schema.get_table_columns("docs")
    )

    pipeline.run(docs())
    assert load_table_counts(pipeline, "docs") == {"docs": 15}

    local_mongo["docs"].insert_many(make_documents(5, start=25))
    pipeline.run(docs())
    assert load_table_counts(pipeline, "docs") == {"docs": 20}


@skip_if_not_active("snowflake")
def test_arrow_nested_types_snowflake() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_nested_types",
        destination=dlt.destinations.snowflake(use_nested_types=True),
        dataset_name="mongodb_nested_types",
        dev_mode=True,
    )
    types_test = mongodb_collection(collection="types_test", data_item_format="arrow")
    # documents are flattened into columns unless nesting is disabled, arrays are always nested
    types_test.max_table_nesting = 0
    pipeline.run(types_test, loader_file_format="parquet")

    with pipeline.sql_client() as client:
        table_name = client.make_qualified_table_name("types_test")
        column_types = {
            name.lower(): data_type
            for name, data_type, *_ in client.execute_sql(f"DESC TABLE {table_name}")
        }
        values = client.execute_sql(
            f"SELECT field7[0], field8['key'] FROM {table_name}"
        )
    assert column_types["field7"] == "ARRAY(NUMBER(19,0))"
    assert column_types["field8"] == "OBJECT(key VARCHAR(16777216))"
    assert values == [(1, "value")]
