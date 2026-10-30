"""Splits a collection into filters on `_id` that read every document exactly once, for parallel
reads. Needs only pymongo."""

import math
from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import OperationFailure

# `$type` aliases of the values `_id` can hold, in the order of the `_id` index
_ID_TYPES = (
    "minKey",
    "null",
    "number",
    "string",
    "symbol",
    "object",
    "binData",
    "objectId",
    "bool",
    "date",
    "timestamp",
    "dbPointer",
    "javascript",
    "javascriptWithScope",
    "maxKey",
)
_NUMBER_TYPES = ("double", "int", "long", "decimal")
# types split into `_id` ranges, values of other types are read in one partition per type:
# `$gte` and `$lt` match only values of their own BSON type
_RANGE_ID_TYPES = ("objectId", "string", "number", "date", "binData")


def plan_partitions(
    collection: Any, partition_size: int, max_partitions: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Split the collection into filters on `_id` that match every document exactly once.

    Args:
        collection (Collection): The collection to split.
        partition_size (int): The number of documents in a range.
        max_partitions (Optional[int]): The maximum number of ranges.

    Returns:
        List[Dict[str, Any]]: Filters on `_id`, a single empty filter when the collection is not
            split: views and time series collections, which have no `_id` index, and collections
            smaller than two partitions.
    """
    try:
        options = collection.options()
    except OperationFailure:
        # without `listCollections` a view or time series collection is split too, only slower
        options = {}
    if "viewOn" in options or "timeseries" in options:
        return [{}]
    count = collection.estimated_document_count()
    ranges = math.ceil(count / partition_size)
    if max_partitions:
        ranges = min(ranges, max_partitions)
    if ranges < 2:
        return [{}]

    id_types = _id_types(collection)
    sample_size = _sample_size(ranges, count)
    range_types = [
        id_type
        for id_type in id_types
        if id_type in _RANGE_ID_TYPES
        # symbols sort with strings, a string range would also match them
        and not (id_type == "string" and "symbol" in id_types)
    ]
    bounds: List[Any] = []
    if range_types:
        range_type = range_types[0]
        if len(range_types) > 1:
            sampled = collection.aggregate(
                [
                    {"$sample": {"size": sample_size}},
                    {"$group": {"_id": {"$type": "$_id"}, "count": {"$sum": 1}}},
                ]
            )
            counts = {_id_type_alias(group["_id"]): group["count"] for group in sampled}
            range_type = max(range_types, key=lambda id_type: counts.get(id_type, 0))
        buckets = list(
            collection.aggregate(
                [
                    {"$sample": {"size": sample_size}},
                    {"$match": {"_id": {"$type": range_type}}},
                    {"$bucketAuto": {"groupBy": "$_id", "buckets": ranges}},
                ]
            )
        )
        bounds = [bucket["_id"]["min"] for bucket in buckets[1:]]
    if not bounds:
        if len(id_types) < 2:
            return [{}]
        return [{"_id": {"$type": id_type}} for id_type in id_types]

    # `$lt` matched NaN before MongoDB 7.1
    first: Dict[str, Any] = {"$gte": float("-inf")} if range_type == "number" else {}
    partitions: List[Dict[str, Any]] = [{"_id": {**first, "$lt": bounds[0]}}]
    partitions.extend(
        {"_id": {"$gte": low, "$lt": high}} for low, high in zip(bounds, bounds[1:])
    )
    partitions.append({"_id": {"$gte": bounds[-1]}})
    # NaN matches no range
    if (
        range_type == "number"
        and collection.find_one({"_id": float("nan")}, {"_id": 1}) is not None
    ):
        partitions.append({"_id": float("nan")})
    partitions.extend(
        {"_id": {"$type": id_type}} for id_type in id_types if id_type != range_type
    )
    return partitions


def and_filters(*filters: Dict[str, Any]) -> Dict[str, Any]:
    present = [filter_ for filter_ in filters if filter_]
    if len(present) > 1:
        return {"$and": present}
    return present[0] if present else {}


def _id_types(collection: Any) -> List[str]:
    """`$type` aliases of the `_id` values in the collection, read from both ends of the `_id`
    index and probed in between"""
    ends = [_id_type_at(collection, direction) for direction in (ASCENDING, DESCENDING)]
    if ends[0] is None or ends[1] is None:
        return []
    first, last = _ID_TYPES.index(ends[0]), _ID_TYPES.index(ends[1])
    # strings and symbols sort together
    if _ID_TYPES[first] == "symbol":
        first -= 1
    if _ID_TYPES[last] == "string":
        last += 1
    return [
        id_type
        for id_type in _ID_TYPES[first : last + 1]
        if id_type in ends
        or collection.find_one({"_id": {"$type": id_type}}, {"_id": 1}) is not None
    ]


def _id_type_at(collection: Any, direction: int) -> Optional[str]:
    first = list(
        collection.aggregate(
            [
                {"$sort": {"_id": direction}},
                {"$limit": 1},
                {"$project": {"type": {"$type": "$_id"}}},
            ]
        )
    )
    return _id_type_alias(first[0]["type"]) if first else None


def _id_type_alias(type_name: str) -> str:
    return "number" if type_name in _NUMBER_TYPES else type_name


def _sample_size(ranges: int, count: int) -> int:
    # `$sample` of more than 5% of the documents scans and sorts the collection
    return max(ranges + 1, min(10 * ranges, count * 4 // 100))
