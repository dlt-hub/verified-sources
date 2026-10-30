import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set
from unittest import mock

import bson
import dlt
from bson.datetime_ms import DatetimeMS
import pyarrow
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources import DltResource
from pymongo.collation import Collation
from pymongo.database import Database

from sources.mongodb import mongodb, mongodb_collection
from sources.mongodb.partition import plan_partitions
from sources.mongodb_pipeline import (
    load_entire_database,
    load_select_collection_db,
    load_select_collection_db_filtered,
)
from tests.mongodb.conftest import LOCAL_MONGO_URL
from tests.mongodb.utils import (
    ProcessRecorder,
    make_documents,
    make_mixed_id_documents,
    server_reads,
)
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


_NUMBERS = list(range(-1, 7))


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize(
    "incremental_args,filter_,expected,returned",
    [
        (dict(initial_value=2), None, [2, 3, 4, 5, 6], 5),
        (dict(initial_value=0), None, [0, 1, 2, 3, 4, 5, 6], 7),
        (dict(initial_value=2, end_value=5), None, [2, 3, 4], 3),
        (
            dict(initial_value=2, end_value=5, range_start="open", range_end="closed"),
            None,
            [3, 4, 5],
            3,
        ),
        (dict(initial_value=5, end_value=2, last_value_func=min), None, [3, 4, 5], 3),
        (
            dict(
                initial_value=5,
                end_value=2,
                last_value_func=min,
                range_start="open",
                range_end="closed",
            ),
            None,
            [2, 3, 4],
            3,
        ),
        (
            dict(initial_value=2, on_cursor_value_missing="include"),
            None,
            [2, 3, 4, 5, 6, None],
            6,
        ),
        (dict(initial_value=2), {"n": {"$exists": True}}, [2, 3, 4, 5, 6], 5),
        (dict(), None, _NUMBERS, 8),
        # dlt filters with other functions than `min` and `max`
        (
            dict(initial_value=2, last_value_func=lambda values: max(values)),
            None,
            [2, 3, 4, 5, 6],
            8,
        ),
    ],
    ids=[
        "max",
        "zero_start",
        "max_end",
        "max_open_start_closed_end",
        "min_end",
        "min_open_start_closed_end",
        "include_missing",
        "user_filter_on_cursor",
        "no_initial_value",
        "custom_last_value_func",
    ],
)
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_local_incremental_server_filter(
    profiled_mongo: Database,
    incremental_args: Dict[str, Any],
    filter_: Optional[Dict[str, Any]],
    expected: List[Optional[int]],
    returned: int,
    data_item_format: str,
) -> None:
    if data_item_format == "arrow" and incremental_args.get("last_value_func") not in (
        None,
        max,
        min,
    ):
        pytest.skip("dlt supports only min and max on arrow tables")
    documents: List[Dict[str, Any]] = [{"n": n} for n in _NUMBERS]
    if "on_cursor_value_missing" in incremental_args:
        documents.append({"m": 1})
    profiled_mongo["numbers"].insert_many(documents)

    items = mongodb_collection(
        connection_url=LOCAL_MONGO_URL,
        database=profiled_mongo.name,
        collection="numbers",
        incremental=dlt.sources.incremental("n", **incremental_args),
        filter_=filter_,
        data_item_format=data_item_format,
    )

    if data_item_format == "arrow":
        loaded = [value for table in items for value in table["n"].to_pylist()]
    else:
        loaded = [item.get("n") for item in items]
    assert sorted(loaded, key=lambda value: (value is None, value)) == expected
    # dlt filters again, the server must return only what the incremental loads
    reads = server_reads(profiled_mongo, "numbers")
    assert sum(read["nreturned"] for read in reads) == returned


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize(
    "cursor,projection,present,absent",
    [
        ("n", ["title"], {"_id", "title", "n"}, {"payload"}),
        # the incremental deduplicates by `_id`
        ("n", {"title": 1, "_id": 0}, {"_id", "title", "n"}, {"payload"}),
        ("n", {"title": 0, "n": 0}, {"_id", "n", "payload"}, {"title"}),
        ("n", {"_id": 1, "title": 0}, {"_id", "n"}, {"title"}),
        # the server rejects a projection of `ref` together with `ref.x`
        ("ref.x", ["ref"], {"_id", "ref"}, {"n", "title"}),
        (
            "ref.x",
            {"ref": 0},
            "excludes `ref` that holds the cursor field `ref.x`",
            None,
        ),
    ],
    ids=[
        "list_inclusion",
        "dict_inclusion",
        "exclusion",
        "exclusion_with_id",
        "parent_included",
        "parent_excluded",
    ],
)
def test_local_projection_keeps_cursor_field(
    local_mongo: Database,
    cursor: str,
    projection: Any,
    present: Any,
    absent: Optional[Set[str]],
) -> None:
    docs = mongodb_collection(
        connection_url=LOCAL_MONGO_URL,
        database=local_mongo.name,
        collection="docs",
        incremental=dlt.sources.incremental(cursor),
        projection=projection,
    )

    if isinstance(present, str):
        with pytest.raises(Exception, match=present):
            list(docs)
        return
    documents = list(docs)
    assert len(documents) == 25
    for document in documents:
        assert present <= set(document) and not absent & set(document)


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize("limit", [None, 3], ids=["no_limit", "limit"])
def test_local_parallel_single_partition(
    profiled_mongo: Database, data_item_format: str, limit: Optional[int]
) -> None:
    # the 25 documents of `docs` are fewer than two partitions
    items = mongodb_collection(
        connection_url=LOCAL_MONGO_URL,
        database=profiled_mongo.name,
        collection="docs",
        parallel=True,
        chunk_size=25,
        limit=limit,
        filter_={"n": {"$lt": 10}},
        data_item_format=data_item_format,
    )

    rows = sum(item.num_rows if data_item_format == "arrow" else 1 for item in items)
    assert rows == (limit or 10)
    # one query with no `_id` range
    queries = [
        read for read in server_reads(profiled_mongo, "docs") if read["op"] == "query"
    ]
    assert [query["command"]["filter"] for query in queries] == [{"n": {"$lt": 10}}]


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_local_dates_out_of_python_range(
    local_mongo: Database, data_item_format: str
) -> None:
    # year 0 and year 10000 do not fit python datetime
    millis = [-62198755200000, 253402300800000]
    local_mongo["dates"].insert_many(
        [{"n": n, "d": DatetimeMS(value)} for n, value in enumerate(millis)]
    )

    items = list(
        mongodb_collection(
            connection_url=LOCAL_MONGO_URL,
            database=local_mongo.name,
            collection="dates",
            data_item_format=data_item_format,
        )
    )

    if data_item_format == "object":
        assert [item["d"] for item in items] == [
            datetime.min.replace(tzinfo=timezone.utc),
            # BSON dates have millisecond precision
            datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
        ]
    else:
        assert items[0]["d"].cast(pyarrow.int64()).to_pylist() == millis


def _seeded_rows_loaded(pipeline: dlt.Pipeline) -> int:
    return pipeline.last_trace.last_normalize_info.row_counts.get("seeded", 0)


def _seeded_ids(pipeline: dlt.Pipeline) -> List[str]:
    with pipeline.sql_client() as client:
        return sorted(row[0] for row in client.execute_sql("SELECT _id FROM seeded"))


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize("parallel", [False, True], ids=["sequential", "parallel"])
def test_local_seeded_full_and_incremental(
    local_mongo: Database,
    seeded_documents: List[Dict[str, Any]],
    data_item_format: str,
    parallel: bool,
) -> None:
    updated_at = [document["updated_at"] for document in seeded_documents]
    seeded_ids = sorted(str(document["_id"]) for document in seeded_documents)

    def seeded(**incremental_args: Any) -> DltResource:
        return mongodb_collection(
            connection_url=LOCAL_MONGO_URL,
            database=local_mongo.name,
            collection="seeded",
            incremental=dlt.sources.incremental("updated_at", **incremental_args),
            parallel=parallel,
            chunk_size=200,
            data_item_format=data_item_format,
        )

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_local_seeded", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(seeded()))
    assert _seeded_rows_loaded(pipeline) == len(seeded_documents)
    assert _seeded_ids(pipeline) == seeded_ids

    # the document with the last value is read again and dropped
    pipeline.run(seeded())
    assert _seeded_rows_loaded(pipeline) == 0
    assert load_table_counts(pipeline, "seeded") == {"seeded": len(seeded_documents)}

    months = [datetime(2024, month, 1, tzinfo=timezone.utc) for month in (1, 2, 3, 4)]
    # with `min` the windows go back in time, the last one ends before the first seeded value
    for last_value_func, bounds in (
        (max, months),
        (min, months[:0:-1] + [datetime(2023, 12, 31, tzinfo=timezone.utc)]),
    ):
        backfill = dlt.pipeline(
            pipeline_name=f"mongodb_local_seeded_backfill_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        for start, end in zip(bounds, bounds[1:]):
            expected = sum(
                start <= value < end if last_value_func is max else end < value <= start
                for value in updated_at
            )
            assert expected > 0
            assert_load_info(
                backfill.run(
                    seeded(
                        initial_value=start,
                        end_value=end,
                        last_value_func=last_value_func,
                        range_start="closed",
                        range_end="open",
                    )
                )
            )
            assert _seeded_rows_loaded(backfill) == expected
        # windows with an end value do not store incremental state
        assert "sources" not in backfill.state
        assert _seeded_ids(backfill) == seeded_ids


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize(
    "last_value_func,initial_value",
    [
        (max, None),
        (min, None),
        (max, datetime(1999, 1, 1, tzinfo=timezone.utc)),
        (min, datetime(2100, 1, 1, tzinfo=timezone.utc)),
    ],
    ids=["max", "min", "max_initial_value", "min_initial_value"],
)
def test_local_seeded_chunks(
    local_mongo: Database,
    seeded_documents: List[Dict[str, Any]],
    data_item_format: str,
    last_value_func: Any,
    initial_value: Optional[datetime],
) -> None:
    chunk_size = 200
    expected = sorted(
        (document["updated_at"] for document in seeded_documents),
        reverse=last_value_func is min,
    )
    # not `parallel`: dlt's incremental also sees the batches in flight that `add_limit` drops
    seeded = mongodb_collection(
        connection_url=LOCAL_MONGO_URL,
        database=local_mongo.name,
        collection="seeded",
        incremental=dlt.sources.incremental(
            "updated_at",
            initial_value=initial_value,
            # in the order of `last_value_func`, newest first with `min`
            row_order="asc",
            last_value_func=last_value_func,
            range_start="open",  # seeded values are unique, the last one is not read again
        ),
        chunk_size=chunk_size,
        data_item_format=data_item_format,
    )
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_local_seeded_chunks", destination="duckdb", dev_mode=True
    )

    for start in range(0, len(expected), chunk_size):
        chunk = expected[start : start + chunk_size]
        assert_load_info(pipeline.run(seeded.add_limit(1)))
        assert _seeded_rows_loaded(pipeline) == len(chunk)
        assert seeded.state["incremental"]["updated_at"]["last_value"] == chunk[-1]
    pipeline.run(seeded)
    assert _seeded_rows_loaded(pipeline) == 0

    with pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT n, updated_at FROM seeded")
        (distinct_ids,) = client.execute_sql("SELECT COUNT(DISTINCT _id) FROM seeded")[
            0
        ]
    assert sorted(rows) == sorted(
        (document["n"], document["updated_at"]) for document in seeded_documents
    )
    assert distinct_ids == len(seeded_documents)
    # checked last so that rows skipped or repeated above surface first
    assert seeded.state["incremental"]["updated_at"]["last_value"].tzinfo is not None


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("row_order", ["asc", "desc", None])
@pytest.mark.parametrize(
    "data_item_format,last_value_func",
    [
        ("object", min),
        ("object", max),
        ("object", lambda values: max(values)),
        ("arrow", min),
        ("arrow", max),
    ],
    ids=["object-min", "object-max", "object-custom", "arrow-min", "arrow-max"],
)
def test_local_seeded_order(
    local_mongo: Database,
    seeded_documents: List[Dict[str, Any]],
    row_order: Optional[str],
    data_item_format: str,
    last_value_func: Any,
) -> None:
    # before or after all seeded values, so every document is read
    initial_value = datetime(
        2100 if last_value_func is min else 1999, 1, 1, tzinfo=timezone.utc
    )
    items = mongodb_collection(
        connection_url=LOCAL_MONGO_URL,
        database=local_mongo.name,
        collection="seeded",
        incremental=dlt.sources.incremental(
            "updated_at",
            initial_value=initial_value,
            last_value_func=last_value_func,
            row_order=row_order,
        ),
        chunk_size=200,
        data_item_format=data_item_format,
    )

    if data_item_format == "arrow":
        values = [value for table in items for value in table["updated_at"].to_pylist()]
    else:
        values = [item["updated_at"] for item in items]

    assert len(values) == len(seeded_documents)
    # the server sorts only with `min` or `max`, ascending in the direction of the last value
    if row_order and last_value_func in (min, max):
        ascending = (row_order == "asc") == (last_value_func is max)
        assert values == sorted(values, reverse=not ascending)


def _parallel_n(
    database: Database, collection: str, data_item_format: str, **kwargs: Any
) -> List[int]:
    """`n` of the documents read with `parallel` in `_id` partitions of about 50 documents"""
    resource = mongodb_collection(
        connection_url=LOCAL_MONGO_URL,
        database=database.name,
        collection=collection,
        parallel=True,
        chunk_size=50,
        data_item_format=data_item_format,
        **kwargs,
    )
    loaded: List[int] = []
    for item in resource:
        loaded += item["n"].to_pylist() if data_item_format == "arrow" else [item["n"]]
    return loaded


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize(
    "majority", ["objectId", "number", "binData", "date", "string"]
)
def test_local_parallel_mixed_ids(
    local_mongo: Database,
    data_item_format: str,
    majority: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EXTRACT__WORKERS", "3")
    documents = make_mixed_id_documents(majority, 600)
    # string `_id`s compare case insensitively in the index, the buckets and the ranges
    collation = Collation(locale="en", strength=2) if majority == "string" else None
    collection = local_mongo.create_collection("mixed", collation=collation)
    collection.insert_many(documents)

    partitions = plan_partitions(collection, 50)
    conditions = [p["_id"] for p in partitions if isinstance(p["_id"], dict)]
    ranges = [condition for condition in conditions if "$type" not in condition]
    types = {condition["$type"] for condition in conditions if "$type" in condition}
    assert len(ranges) > 2
    assert majority not in types and {"minKey", "null", "maxKey", "object"} <= types
    # NaN matches no number range
    assert (majority == "number") == any(
        isinstance(p["_id"], float) for p in partitions
    )

    for filter_ in ({}, {"n": {"$mod": [3, 0]}}):
        loaded = _parallel_n(local_mongo, "mixed", data_item_format, filter_=filter_)
        assert sorted(loaded) == sorted(
            document["n"]
            for document in documents
            if not filter_ or document["n"] % 3 == 0
        ), filter_


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_local_parallel_view_and_timeseries(
    local_mongo: Database, data_item_format: str
) -> None:
    local_mongo["base"].insert_many(make_mixed_id_documents("objectId", 300))
    local_mongo.command(
        "create", "base_view", viewOn="base", pipeline=[{"$match": {"n": {"$lt": 100}}}]
    )
    local_mongo.create_collection("series", timeseries={"timeField": "ts"})
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    local_mongo["series"].insert_many(
        [{"ts": start + timedelta(minutes=n), "n": n} for n in range(300)]
    )

    # no `_id` index to split by: read in one partition
    for name, expected in (("base_view", range(100)), ("series", range(300))):
        assert plan_partitions(local_mongo[name], 50) == [{}]
        assert sorted(_parallel_n(local_mongo, name, data_item_format)) == list(
            expected
        )


@skipifwindows
@skip_if_not_active("duckdb")
def test_local_parallel_concurrent_deletes(
    local_mongo: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EXTRACT__WORKERS", "2")
    documents = make_mixed_id_documents("objectId", 2000)
    local_mongo["changing"].insert_many(documents)
    resource = mongodb_collection(
        connection_url=LOCAL_MONGO_URL,
        database=local_mongo.name,
        collection="changing",
        parallel=True,
        chunk_size=50,
    )

    loaded: List[int] = []
    for document in resource:
        if not loaded:
            # skip and limit windows read after the delete would shift and miss documents
            local_mongo["changing"].delete_many({"n": {"$mod": [3, 0]}})
        loaded.append(document["n"])

    assert len(loaded) == len(set(loaded))
    assert {d["n"] for d in documents if d["n"] % 3} <= set(loaded)


@skipifwindows
@skip_if_not_active("duckdb")
def test_local_parallel_arrow_workers(
    local_mongo: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EXTRACT__WORKERS", "2")
    local_mongo["many"].insert_many(make_documents(300))
    recorder = ProcessRecorder(monkeypatch)

    def many(**kwargs: Any) -> DltResource:
        return mongodb_collection(
            connection_url=LOCAL_MONGO_URL,
            database=local_mongo.name,
            collection="many",
            parallel=True,
            chunk_size=50,
            data_item_format="arrow",
            **kwargs,
        )

    # one partition; the workers stop with the extract threads
    rows = sum(table.num_rows for table in many().add_limit(1))
    assert 0 < rows < 300
    assert 1 <= len(recorder.processes) <= 2
    assert recorder.running == 0

    # an error of the server is raised with the traceback of the worker
    with pytest.raises(Exception) as ex:
        list(many(filter_={"$bogus": 1}))
    assert "unknown top level operator: $bogus" in str(ex.value)
    assert recorder.running == 0


@skipifwindows
@skip_if_not_active("duckdb")
@pytest.mark.parametrize("next_item_mode", ["round_robin", "fifo"])
def test_local_parallel_arrow_workers_shared(
    local_mongo: Database, monkeypatch: pytest.MonkeyPatch, next_item_mode: str
) -> None:
    monkeypatch.setenv("EXTRACT__WORKERS", "2")
    monkeypatch.setenv("EXTRACT__NEXT_ITEM_MODE", next_item_mode)
    names = ["many_a", "many_b", "many_c"]
    for name in names:
        local_mongo[name].insert_many(make_documents(300))
    recorder = ProcessRecorder(monkeypatch)

    source = mongodb(
        connection_url=LOCAL_MONGO_URL,
        database=local_mongo.name,
        collection_names=names,
        parallel=True,
        chunk_size=50,
        data_item_format="arrow",
    )
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_local_shared_workers",
        destination="duckdb",
        dev_mode=True,
    )
    assert_load_info(pipeline.run(source))

    # one process per extract thread for all collections, stopped with the threads
    assert 1 <= recorder.most_running <= 2
    assert recorder.running == 0
    with pipeline.sql_client() as client:
        for name in names:
            assert client.execute_sql(
                f"SELECT COUNT(*), COUNT(DISTINCT n) FROM {name}"
            ) == [(300, 300)]


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
