import warnings
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dlt
import pyarrow as pa
import pytest
from bson import DBRef, Decimal128, MaxKey, MinKey, ObjectId, Regex, Timestamp
from bson.datetime_ms import DatetimeMS

from sources.mongodb.helpers import (
    COMPRESSORS,
    client_from_credentials,
    client_options,
    convert_mongo_objs,
)

from tests.mongodb.utils import FakeCollection, FakeCursor, extract, make_documents


def _query(collection: FakeCollection, **kwargs: Any) -> FakeCursor:
    extract(collection, **kwargs)
    return collection.cursors[0]


def test_convert_mongo_objs() -> None:
    oid = ObjectId()
    assert convert_mongo_objs(oid) == str(oid)
    assert convert_mongo_objs(Decimal128("1.10")) == "1.10"
    assert convert_mongo_objs(Regex("^foo", "i")) == "^foo"
    # PCRE syntax that python `re` does not compile
    assert convert_mongo_objs(Regex(r"(?<name>a)\p{L}", "i")) == r"(?<name>a)\p{L}"
    assert convert_mongo_objs(DBRef("users", oid, "db")) == {
        "$ref": "users",
        "$id": str(oid),
        "$db": "db",
    }
    assert convert_mongo_objs(MinKey()) is None
    assert convert_mongo_objs(MaxKey()) is None
    converted = convert_mongo_objs(Timestamp(1704067200, 1))
    assert converted == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert converted.tzinfo == timezone.utc


@pytest.mark.parametrize(
    "incremental_args,expected",
    [
        (dict(initial_value=2), {"n": {"$gte": 2}}),
        (dict(initial_value=0), {"n": {"$gte": 0}}),
        (dict(initial_value=2, end_value=5), {"n": {"$gte": 2, "$lt": 5}}),
        (
            dict(initial_value=2, end_value=5, range_start="open", range_end="closed"),
            {"n": {"$gt": 2, "$lte": 5}},
        ),
        (
            dict(initial_value=5, end_value=2, last_value_func=min),
            {"n": {"$lte": 5, "$gt": 2}},
        ),
        (
            dict(
                initial_value=5,
                end_value=2,
                last_value_func=min,
                range_start="open",
                range_end="closed",
            ),
            {"n": {"$lt": 5, "$gte": 2}},
        ),
        (
            dict(initial_value=2, on_cursor_value_missing="include"),
            {"$or": [{"n": {"$gte": 2}}, {"n": None}]},
        ),
        (dict(), {}),
        (dict(initial_value=2, last_value_func=lambda values: max(values)), {}),
    ],
    ids=[
        "max",
        "zero_start",
        "max_end",
        "max_open_start_closed_end",
        "min_end",
        "min_open_start_closed_end",
        "include_missing",
        "no_initial_value",
        "custom_last_value_func",
    ],
)
@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize("parallel", [False, True], ids=["sequential", "parallel"])
def test_incremental_filter(
    incremental_args: Dict[str, Any],
    expected: Dict[str, Any],
    data_item_format: str,
    parallel: bool,
) -> None:
    if data_item_format == "arrow" and incremental_args.get("last_value_func") not in (
        None,
        max,
        min,
    ):
        pytest.skip("dlt supports only min and max on arrow tables")
    collection = FakeCollection("docs", make_documents(6))

    cursor = _query(
        collection,
        incremental=dlt.sources.incremental("n", **incremental_args),
        data_item_format=data_item_format,
        parallel=parallel,
    )

    assert cursor.filter == expected


def test_incremental_filter_with_user_filter() -> None:
    collection = FakeCollection("docs", make_documents(6))

    cursor = _query(
        collection,
        incremental=dlt.sources.incremental("n", initial_value=2),
        filter_={"n": {"$exists": True}},
    )
    assert cursor.filter == {"$and": [{"n": {"$gte": 2}}, {"n": {"$exists": True}}]}

    with pytest.raises(Exception) as ex:
        extract(
            FakeCollection("docs", []),
            incremental=dlt.sources.incremental("n", initial_value=2),
            filter_={"n": {"$gte": 3}},
        )
    assert "already used by the incremental" in str(ex.value)


@pytest.mark.parametrize(
    "row_order,last_value_func,expected",
    [
        ("asc", max, [("n", 1)]),
        ("desc", min, [("n", 1)]),
        ("asc", min, [("n", -1)]),
        ("desc", max, [("n", -1)]),
        (None, max, []),
    ],
    ids=["asc_max", "desc_min", "asc_min", "desc_max", "no_row_order"],
)
def test_incremental_sort(
    row_order: Optional[str], last_value_func: Any, expected: Any
) -> None:
    collection = FakeCollection("docs", make_documents(6))

    # sorts also on the first run, without a last value
    cursor = _query(
        collection,
        incremental=dlt.sources.incremental(
            "n", row_order=row_order, last_value_func=last_value_func  # type: ignore[arg-type]
        ),
    )

    assert cursor.sort_spec == expected


@pytest.mark.parametrize(
    "projection,expected",
    [
        (["title"], {"title": 1, "n": 1}),
        ({"title": 1, "_id": 0}, {"title": 1, "_id": 0, "n": 1}),
        ({"title": 0, "n": 0}, {"title": 0}),
        ({"_id": 1, "title": 0}, {"_id": 1, "title": 0}),
    ],
    ids=["list_inclusion", "dict_inclusion", "exclusion", "exclusion_with_id"],
)
def test_projection_keeps_cursor_field(
    projection: Any, expected: Dict[str, Any]
) -> None:
    collection = FakeCollection("docs", make_documents(2))

    cursor = _query(
        collection, projection=projection, incremental=dlt.sources.incremental("n")
    )
    assert cursor.projection == expected

    collection = FakeCollection("docs", make_documents(2))
    assert _query(collection, projection=["title"]).projection == {"title": 1}


@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
@pytest.mark.parametrize("limit", [None, 3], ids=["no_limit", "limit"])
def test_parallel_single_partition(data_item_format: str, limit: Optional[int]) -> None:
    documents = make_documents(5)
    collection = FakeCollection("docs", documents)

    items = extract(
        collection,
        parallel=True,
        chunk_size=5,
        limit=limit,
        filter_={"n": {"$lt": 10}},
        data_item_format=data_item_format,
    )

    # fewer documents than two partitions: one query with no `_id` range and no skip
    (cursor,) = collection.cursors
    assert cursor.filter == {"n": {"$lt": 10}}
    assert (cursor.skip_count, cursor.limit_count) == (0, limit or 0)
    if data_item_format == "arrow":
        ids = [oid for table in items for oid in table["_id"].to_pylist()]
    else:
        ids = [document["_id"] for document in items]
    assert len(ids) == (limit or len(documents))
    assert set(ids) <= {str(document["_id"]) for document in documents}


@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_dates_out_of_python_range(data_item_format: str) -> None:
    # year 0 and year 10000 do not fit python datetime
    millis = [-62198755200000, 253402300800000]
    collection = FakeCollection(
        "docs", [{"n": n, "d": DatetimeMS(ms)} for n, ms in enumerate(millis)]
    )

    items = extract(collection, data_item_format=data_item_format)

    if data_item_format == "object":
        assert [item["d"] for item in items] == [
            datetime.min.replace(tzinfo=timezone.utc),
            # BSON dates have millisecond precision
            datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
        ]
    else:
        assert items[0]["d"].cast(pa.int64()).to_pylist() == millis


@pytest.mark.parametrize(
    "url,compressors",
    [
        ("mongodb+srv://user:pass@cluster.example.net/db", COMPRESSORS),
        ("mongodb://localhost/?compressors=snappy", None),
        ("mongodb://localhost/?retryWrites=true&Compressors=", None),
    ],
    ids=["default", "from_url", "disabled_in_url"],
)
def test_client_compressors(url: str, compressors: Optional[str]) -> None:
    # the compressors of the URL win, the client options would override them
    assert client_options(url).get("compressors") == compressors


def test_client_skips_missing_compressors() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        client = client_from_credentials("mongodb://localhost:1/?connect=false")
    assert "zlib" in client.options.pool_options._compression_settings.compressors
