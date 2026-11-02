"""Splits a collection into `_id` partitions that read every document exactly once"""

import math
from typing import Any, Callable, Dict, List, Optional, Tuple, TypedDict

from bson.max_key import MaxKey
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import OperationFailure

ID_INDEX = {"_id": 1}
DEFAULT_PARTITION_SIZE_BYTES = 16 * 1024 * 1024
MIN_PARTITION_SIZE_BYTES = 1024 * 1024

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
# `_id` types split into ranges, each other type is read in one partition
_RANGE_ID_TYPES = ("objectId", "string", "number", "date", "binData")
_MAX_KEY_FILTER = {"_id": {"$type": "maxKey"}}


class TPartition(TypedDict, total=False):
    """Arguments of `find` that select the documents of a partition, empty for all documents"""

    # predicates on `_id`
    filter: Dict[str, Any]  # noqa: A003
    # inclusive and exclusive bounds on the `_id` index, which `hint` selects
    min: Dict[str, Any]  # noqa: A003
    max: Dict[str, Any]  # noqa: A003
    hint: Dict[str, int]


def plan_partitions(
    collection: Any,
    filter_: Dict[str, Any],
    partition_size_bytes: int,
    chunk_size: int,
    split_id_types: bool = False,
    min_partitions: int = 1,
) -> List[TPartition]:
    """Splits the documents that match `filter_` into partitions of about `partition_size_bytes`.

    Without a filter, partitions are bounds on the `_id` index. Bounds hold `_id`s of all BSON
    types. With a filter, partitions are `_id` ranges of the most common `_id` type and one
    partition for each other type. A single empty partition reads the collection in one query.

    Args:
        collection (Collection): Collection to split.
        filter_ (Dict[str, Any]): Filter of the read.
        partition_size_bytes (int): Maximum size of a partition.
        chunk_size (int): Number of documents in a partition when the server has no stats.
        split_id_types (bool): Splits `_id` types into separate partitions also without a filter.
        min_partitions (int): Minimum number of partitions of at least `MIN_PARTITION_SIZE_BYTES`.

    Returns:
        List[TPartition]: The partitions.

    Raises:
        OperationFailure: The server rejects a query that plans the partitions.
    """
    try:
        options: Optional[Dict[str, Any]] = collection.options()
    except OperationFailure:
        # without `listCollections` the `_id` index is not known to exist
        options = None
    # views and time series collections have no `_id` index
    if options and ("viewOn" in options or "timeseries" in options):
        return [{}]
    stats = _storage_stats(collection)
    if filter_:
        count = collection.count_documents(filter_)
    elif stats:
        count = stats[0]
    else:
        count = collection.estimated_document_count()
    documents_per_partition = _documents_per_partition(
        stats, count, partition_size_bytes, chunk_size, min_partitions
    )
    ranges = math.ceil(count / documents_per_partition)
    if ranges < 2:
        return [{}]
    sample_size = _sample_size(ranges, count)
    # bounds need a hint on the `_id` index, which keeps the filter from using its own index.
    # clustered collections have no separate `_id` index
    bounds_allowed = (
        not filter_ and options is not None and "clusteredIndex" not in options
    )
    id_types = _id_types(collection) if split_id_types or not bounds_allowed else []
    if bounds_allowed and len(id_types) < 2:
        return _index_bound_partitions(collection, ranges, sample_size)
    return _id_range_partitions(collection, id_types, filter_, ranges, sample_size)


def find_partition(
    find: Callable[..., Any],
    partition: TPartition,
    filter_: Dict[str, Any],
    projection: Optional[Dict[str, Any]],
) -> Any:
    """Cursor of `find` over the documents of `partition` that match `filter_`"""
    bounds = {key: value for key, value in partition.items() if key != "filter"}
    return find(
        filter=and_filters(filter_, partition.get("filter", {})),
        projection=projection,
        **bounds,
    )


def and_filters(*filters: Dict[str, Any]) -> Dict[str, Any]:
    present = [filter_ for filter_ in filters if filter_]
    if len(present) > 1:
        return {"$and": present}
    return present[0] if present else {}


def bounded_partitions(bounds: List[Any]) -> List[TPartition]:
    """Partitions between consecutive `_id` index bounds, unbounded at both ends"""
    # the server rejects a MaxKey bound, `_index_bound_partitions` reads MaxKey `_id`s
    if bounds and isinstance(bounds[-1], MaxKey):
        bounds = bounds[:-1]
    if not bounds:
        return [{}]
    partitions: List[TPartition] = [{"max": {"_id": bounds[0]}, "hint": ID_INDEX}]
    partitions.extend(
        {"min": {"_id": low}, "max": {"_id": high}, "hint": ID_INDEX}
        for low, high in zip(bounds, bounds[1:])
    )
    partitions.append({"min": {"_id": bounds[-1]}, "hint": ID_INDEX})
    return partitions


def range_partitions(
    bounds: List[Any], range_type: str, has_nan: bool, other_types: List[str]
) -> List[TPartition]:
    """Partitions of `_id` ranges of `range_type` between `bounds`, NaN and the other types"""
    # `$lt` matched NaN before MongoDB 7.1
    first: Dict[str, Any] = {"$gte": float("-inf")} if range_type == "number" else {}
    filters: List[Dict[str, Any]] = [{"_id": {**first, "$lt": bounds[0]}}]
    filters.extend(
        {"_id": {"$gte": low, "$lt": high}} for low, high in zip(bounds, bounds[1:])
    )
    filters.append({"_id": {"$gte": bounds[-1]}})
    if has_nan:
        # NaN matches no range
        filters.append({"_id": float("nan")})
    # `$gte` and `$lt` match only values of their own BSON type
    filters.extend({"_id": {"$type": id_type}} for id_type in other_types)
    return [{"filter": filter_} for filter_ in filters]


def _index_bound_partitions(
    collection: Any, ranges: int, sample_size: int
) -> List[TPartition]:
    buckets = list(
        collection.aggregate(
            [
                {"$sample": {"size": sample_size}},
                {"$project": {"_id": 1}},
                {"$bucketAuto": {"groupBy": "$_id", "buckets": ranges}},
            ]
        )
    )
    partitions = bounded_partitions([bucket["_id"]["min"] for bucket in buckets[1:]])
    # an index scan without `max` ends before MaxKey
    if (
        len(partitions) > 1
        and collection.find_one(_MAX_KEY_FILTER, {"_id": 1}) is not None
    ):
        partitions.append({"filter": _MAX_KEY_FILTER})
    return partitions


def _id_range_partitions(
    collection: Any,
    id_types: List[str],
    filter_: Dict[str, Any],
    ranges: int,
    sample_size: int,
) -> List[TPartition]:
    range_types = [
        id_type
        for id_type in id_types
        if id_type in _RANGE_ID_TYPES
        # symbols sort with strings, a string range also matches them
        and not (id_type == "string" and "symbol" in id_types)
    ]
    bounds: List[Any] = []
    if range_types:
        range_type = range_types[0]
        if len(range_types) > 1:
            # ranges of the most common type in a sample
            sampled = collection.aggregate(
                [
                    {"$sample": {"size": sample_size}},
                    {"$group": {"_id": {"$type": "$_id"}, "count": {"$sum": 1}}},
                ]
            )
            counts = {_id_type_alias(group["_id"]): group["count"] for group in sampled}
            range_type = max(range_types, key=lambda id_type: counts.get(id_type, 0))
        # the filtered documents decide the bounds, a `$sample` behind `$match` scans them
        matching = [{"$match": filter_}, {"$project": {"_id": 1}}] if filter_ else []
        buckets = list(
            collection.aggregate(
                [
                    *matching,
                    {"$sample": {"size": sample_size}},
                    {"$project": {"_id": 1}},
                    {"$match": {"_id": {"$type": range_type}}},
                    {"$bucketAuto": {"groupBy": "$_id", "buckets": ranges}},
                ]
            )
        )
        bounds = [bucket["_id"]["min"] for bucket in buckets[1:]]
    if not bounds:
        # too few documents of `range_type` to split
        if len(id_types) < 2:
            return [{}]
        return [{"filter": {"_id": {"$type": id_type}}} for id_type in id_types]

    has_nan = (
        range_type == "number"
        and collection.find_one(and_filters(filter_, {"_id": float("nan")}), {"_id": 1})
        is not None
    )
    other_types = [id_type for id_type in id_types if id_type != range_type]
    return range_partitions(bounds, range_type, has_nan, other_types)


def _storage_stats(collection: Any) -> Optional[Tuple[int, int]]:
    """The count and uncompressed size in bytes of the documents, summed over the shards"""
    try:
        shards = list(collection.aggregate([{"$collStats": {"storageStats": {}}}]))
    except OperationFailure:
        return None
    stats = [shard.get("storageStats", {}) for shard in shards]
    return (
        sum(int(stat.get("count", 0)) for stat in stats),
        sum(int(stat.get("size", 0)) for stat in stats),
    )


def _documents_per_partition(
    stats: Optional[Tuple[int, int]],
    count: int,
    partition_size_bytes: int,
    chunk_size: int,
    min_partitions: int,
) -> int:
    """Number of documents in a partition"""
    if not stats or stats[0] <= 0 or stats[1] <= 0:
        return chunk_size
    all_count, all_size = stats
    # `count` documents at the average document size of the collection
    size = count * all_size // all_count
    # smaller partitions, down to `MIN_PARTITION_SIZE_BYTES`, to get `min_partitions`
    partition_size = min(
        partition_size_bytes, max(MIN_PARTITION_SIZE_BYTES, size // min_partitions)
    )
    return max(1, partition_size * all_count // all_size)


def _id_types(collection: Any) -> List[str]:
    """`$type` aliases of the `_id` values in the collection"""
    # the types at both ends of the `_id` index, the types between them are probed one by one
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
