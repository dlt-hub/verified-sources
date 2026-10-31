import math
from typing import Any, Optional, Tuple

import pytest
from bson import MaxKey, ObjectId
from pymongo.errors import OperationFailure

from sources.mongodb.helpers import planned_partitions
from sources.mongodb.partition import (
    ID_INDEX,
    _documents_per_partition,
    _sample_size,
    _storage_stats,
    bounded_partitions,
    find_partition,
    range_partitions,
)

from tests.mongodb.utils import FakeCollection, make_documents

# splitting collections is tested on the local server in test_mongodb_source.py, mocks of its
# type bracketing, index bounds and `$bucketAuto` would test our assumptions only


@pytest.mark.parametrize(
    "ranges,count,expected",
    [(10, 1_000_000, 100), (10, 1_000, 40), (10, 100, 11)],
    ids=["ten_per_range", "four_percent", "one_more_than_ranges"],
)
def test_sample_size(ranges: int, count: int, expected: int) -> None:
    assert _sample_size(ranges, count) == expected


def test_bounded_partitions() -> None:
    assert bounded_partitions([]) == [{}]
    # a null `_id` is a bound like any other
    assert bounded_partitions([None, 5, "a"]) == [
        {"max": {"_id": None}, "hint": ID_INDEX},
        {"min": {"_id": None}, "max": {"_id": 5}, "hint": ID_INDEX},
        {"min": {"_id": 5}, "max": {"_id": "a"}, "hint": ID_INDEX},
        {"min": {"_id": "a"}, "hint": ID_INDEX},
    ]
    # a MaxKey bound is dropped, the last partition stays open
    assert bounded_partitions([1, MaxKey()]) == [
        {"max": {"_id": 1}, "hint": ID_INDEX},
        {"min": {"_id": 1}, "hint": ID_INDEX},
    ]
    assert bounded_partitions([MaxKey()]) == [{}]


def test_range_partitions() -> None:
    oid = ObjectId()
    assert range_partitions([oid], "objectId", False, []) == [
        {"filter": {"_id": {"$lt": oid}}},
        {"filter": {"_id": {"$gte": oid}}},
    ]

    partitions = range_partitions([5, 10], "number", True, ["null", "string"])
    nan = partitions.pop(3)["filter"]["_id"]
    assert isinstance(nan, float) and math.isnan(nan)
    assert partitions == [
        {"filter": {"_id": {"$gte": float("-inf"), "$lt": 5}}},
        {"filter": {"_id": {"$gte": 5, "$lt": 10}}},
        {"filter": {"_id": {"$gte": 10}}},
        {"filter": {"_id": {"$type": "null"}}},
        {"filter": {"_id": {"$type": "string"}}},
    ]


def test_find_partition() -> None:
    collection = FakeCollection("docs", make_documents(3))

    cursor = find_partition(collection.find, {}, {"n": 1}, None)
    assert (cursor.filter, cursor.projection) == ({"n": 1}, None)
    assert (cursor.hint, cursor.min, cursor.max) == (None, None, None)

    cursor = find_partition(
        collection.find_raw_batches, {"filter": {"_id": {"$lt": 5}}}, {"n": 1}, {"n": 1}
    )
    assert cursor.filter == {"$and": [{"n": 1}, {"_id": {"$lt": 5}}]}
    assert (cursor.projection, cursor.raw_batch_size) == ({"n": 1}, 101)

    cursor = find_partition(
        collection.find,
        {"min": {"_id": 1}, "max": {"_id": 5}, "hint": ID_INDEX},
        {},
        None,
    )
    assert cursor.filter == {}
    assert (cursor.min, cursor.max, cursor.hint) == ({"_id": 1}, {"_id": 5}, ID_INDEX)


def test_storage_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    collection = FakeCollection("docs", make_documents(3))
    assert _storage_stats(collection) is None

    # one document per shard
    monkeypatch.setattr(
        collection,
        "aggregate",
        lambda pipeline: [
            {"shard": "a", "storageStats": {"count": 10, "size": 4000}},
            {"shard": "b", "storageStats": {"count": 30, "size": 8000}},
        ],
    )
    assert _storage_stats(collection) == (40, 12000)


MIB = 1024 * 1024


@pytest.mark.parametrize(
    "stats,count,partition_size_bytes,min_partitions,expected",
    [
        (None, 100, 1024, 1, 7),
        ((0, 0), 100, 1024, 1, 7),
        ((10_000, 100_000_000), 10_000, 16 * MIB, 1, 1677),
        ((10, 10**9), 10, 1024, 1, 1),
        # a collection smaller than a partition makes one
        ((100, 1_000_000), 100, 16 * MIB, 1, 104),
        # 170 MiB for 4 workers: 32 partitions of about 5 MiB instead of 11 of 16 MiB
        ((300_000, 170 * MIB), 300_000, 16 * MIB, 32, 9375),
        # the 100 matching documents of a filter are 2 MiB, no partition is smaller than 1 MiB
        ((1000, 20 * MIB), 100, 16 * MIB, 32, 50),
    ],
    ids=[
        "no_stats",
        "empty",
        "average_size",
        "at_least_one",
        "small_collection",
        "partitions_per_worker",
        "filtered_at_least_one_mib",
    ],
)
def test_documents_per_partition(
    stats: Optional[Tuple[int, int]],
    count: int,
    partition_size_bytes: int,
    min_partitions: int,
    expected: int,
) -> None:
    assert (
        _documents_per_partition(stats, count, partition_size_bytes, 7, min_partitions)
        == expected
    )


def test_planned_partitions_falls_back_to_one_query(mocker: Any) -> None:
    warning = mocker.patch("dlt.common.logger.warning")
    # the fake server has no stats and no `$sample`, 10 documents make 3 partitions of 4
    collection = FakeCollection("docs", make_documents(10))

    assert planned_partitions(collection, {}, 1024, 4) == [{}]

    assert "read in one query" in str(warning.call_args)
    with pytest.raises(OperationFailure):
        collection.aggregate([])
