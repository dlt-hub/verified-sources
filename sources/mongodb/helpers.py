"""Mongo database source helpers"""

import re
import warnings
from datetime import timezone
from itertools import islice
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    Iterable,
    Mapping,
)

import dlt
from bson.codec_options import CodecOptions, DatetimeConversion
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.timestamp import Timestamp
from dlt.common import logger
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.typing import TDataItem
from dlt.common.utils import map_nested_values_in_place
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.errors import OperationFailure

from .partition import (
    DEFAULT_PARTITION_SIZE_BYTES,
    TPartition,
    find_partition,
    plan_partitions,
)

if TYPE_CHECKING:
    TMongoClient = MongoClient[Any]
    TCollection = Collection[Any]
    TCursor = Cursor[Any]
    TCodecOptions = CodecOptions[Any]
else:
    TMongoClient = Any
    TCollection = Any
    TCursor = Any
    TCodecOptions = Any

TBackend = Literal["python", "pyarrow"]
TTypeMismatch = Literal["raise", "warn", "ignore"]

# compression of the documents sent by the server, the first one the server supports is used
COMPRESSORS = "zstd,snappy,zlib"
# partitions of a parallel read per extract worker, so that the last round leaves few idle
PARTITIONS_PER_WORKER = 8


class CollectionLoader:
    def __init__(
        self,
        client: TMongoClient,
        collection: TCollection,
        chunk_size: int,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> None:
        self.client = client
        self.collection = collection
        self.incremental = incremental
        self.chunk_size = chunk_size

    @property
    def _sort_op(self) -> List[Tuple[str, int]]:
        incremental = self.incremental
        if not incremental or not incremental.row_order:
            return []
        if incremental.last_value_func not in (max, min):
            return []

        ascending = (incremental.row_order == "asc") == (
            incremental.last_value_func is max
        )
        return [(incremental.cursor_path, ASCENDING if ascending else DESCENDING)]

    @property
    def _filter_op(self) -> Dict[str, Any]:
        """The filter of the incremental range"""
        incremental = self.incremental
        if not incremental or incremental.last_value_func not in (max, min):
            return {}
        start = incremental.last_value
        if start is None:
            return {}
        end = incremental.end_value
        start_closed = incremental.range_start == "closed"
        end_closed = incremental.range_end == "closed"

        if incremental.last_value_func is max:
            cursor_filter = {"$gte" if start_closed else "$gt": start}
            if end is not None:
                cursor_filter["$lte" if end_closed else "$lt"] = end
        else:
            cursor_filter = {"$lte" if start_closed else "$lt": start}
            if end is not None:
                cursor_filter["$gte" if end_closed else "$gt"] = end

        cursor_path = incremental.cursor_path
        if incremental.on_cursor_value_missing == "include":
            return {"$or": [{cursor_path: cursor_filter}, {cursor_path: None}]}
        return {cursor_path: cursor_filter}

    def _query_filter(self, filter_: Dict[str, Any]) -> Dict[str, Any]:
        """The incremental filter and `filter_` combined"""
        filter_op = self._filter_op
        _raise_if_intersection(filter_op, filter_)
        if filter_op and filter_:
            return {"$and": [filter_op, filter_]}
        return filter_op or filter_

    def _projection_op(
        self, projection: Optional[Union[Mapping[str, Any], Iterable[str]]]
    ) -> Optional[Dict[str, Any]]:
        """The projection with the cursor field of the incremental"""
        if projection is None:
            return None

        if isinstance(projection, Mapping):
            projection_dict = dict(projection)
        else:
            projection_dict = {field: 1 for field in projection}

        if self.incremental:
            cursor_path = self.incremental.cursor_path
            # an inclusion projection can exclude `_id` and an exclusion projection can include it
            if any(
                value == 1 for field, value in projection_dict.items() if field != "_id"
            ):
                # MongoDB rejects a projection of a document together with its fields
                if not any(
                    _is_path_prefix(field, cursor_path)
                    for field, value in projection_dict.items()
                    if value == 1
                ):
                    projection_dict[cursor_path] = 1
            else:
                for field in projection_dict:
                    if field != cursor_path and _is_path_prefix(field, cursor_path):
                        raise ValueError(
                            f"Projection excludes `{field}` that holds the cursor field "
                            f"`{cursor_path}`. Remove `{field}` from the projection."
                        )
                if projection_dict.pop(cursor_path, None) is not None:
                    logger.warning(
                        f"Cursor field `{cursor_path}` is removed from the exclusion "
                        "projection because the incremental needs it."
                    )

        return projection_dict

    def _limit(self, cursor: TCursor, limit: Optional[int] = None) -> TCursor:
        if limit not in (0, None):
            if not self._sort_op:
                logger.warning(
                    "`limit` without `row_order` of the incremental loads documents in no "
                    "defined order. Set `row_order` to load the first documents."
                )

            cursor = cursor.limit(abs(limit))

        return cursor

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> Iterator[TDataItem]:
        """Yields lists of `chunk_size` documents"""
        cursor = self.collection.find(
            filter=self._query_filter(filter_),
            projection=self._projection_op(projection),
        )
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)

        cursor = self._limit(cursor, limit)

        while docs_slice := list(islice(cursor, self.chunk_size)):
            yield map_nested_values_in_place(convert_mongo_objs, docs_slice)


class CollectionLoaderParallel(CollectionLoader):
    def __init__(
        self,
        client: TMongoClient,
        collection: TCollection,
        chunk_size: int,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
        partition_size_bytes: int = DEFAULT_PARTITION_SIZE_BYTES,
    ) -> None:
        super().__init__(client, collection, chunk_size, incremental)
        self.partition_size_bytes = partition_size_bytes

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> Iterator[TDataItem]:
        """Yields a deferred read of each `_id` partition"""
        if limit:
            # partitions do not tell which documents come first
            yield from super().load_documents(filter_, limit, projection)
            return
        query_filter = self._query_filter(filter_)
        projection_op = self._projection_op(projection)
        for partition in planned_partitions(
            self.collection, query_filter, self.partition_size_bytes, self.chunk_size
        ):
            yield self._read_partition(partition, query_filter, projection_op)

    @dlt.defer
    def _read_partition(
        self,
        partition: TPartition,
        filter_: Dict[str, Any],
        projection: Optional[Dict[str, Any]],
    ) -> TDataItem:
        documents = list(
            find_partition(self.collection.find, partition, filter_, projection)
        )
        return map_nested_values_in_place(convert_mongo_objs, documents)


def planned_partitions(
    collection: TCollection,
    filter_: Dict[str, Any],
    partition_size_bytes: int,
    chunk_size: int,
    split_id_types: bool = False,
) -> List[TPartition]:
    """`plan_partitions` with `PARTITIONS_PER_WORKER` partitions for each extract worker"""
    # a `workers` argument of `pipeline.extract` is not in the config, only the `extract` section
    workers = dlt.config.get("extract.workers", int) or 5
    try:
        return plan_partitions(
            collection,
            filter_,
            partition_size_bytes,
            chunk_size,
            split_id_types,
            min_partitions=PARTITIONS_PER_WORKER * workers,
        )
    except OperationFailure as ex:
        # the server rejects `$where` in `filter_`, old servers do not have `$bucketAuto`
        logger.warning(
            f"Collection `{collection.name}` is read in one query because the server rejected "
            f"the query that plans its partitions: {ex}"
        )
        return [{}]


def collection_documents(
    client: TMongoClient,
    collection: TCollection,
    filter_: Dict[str, Any],
    projection: Optional[Union[Mapping[str, Any], Iterable[str]]],
    pymongoarrow_schema: Any,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    parallel: bool = False,
    limit: Optional[int] = None,
    chunk_size: Optional[int] = 10000,
    backend: TBackend = "python",
    on_inferred_type_mismatch: TTypeMismatch = "ignore",
    connection_url: Optional[str] = None,
    partition_size_bytes: int = DEFAULT_PARTITION_SIZE_BYTES,
) -> Iterator[TDataItem]:
    """Yields the documents of `collection`, see `mongodb` for the arguments"""
    if backend == "pyarrow" and not _pymongoarrow_available():
        logger.warning(
            '`pymongoarrow` is not installed, the "python" backend loads the documents. '
            'Install `pymongoarrow` to use the "pyarrow" backend.'
        )
        backend = "python"

    if backend != "pyarrow" and pymongoarrow_schema:
        logger.warning(
            '`pymongoarrow_schema` is ignored by the "python" backend. Use '
            '`backend="pyarrow"` to apply it.'
        )

    if backend == "pyarrow":
        from .arrow import (
            CollectionArrowLoader,
            CollectionArrowLoaderParallel,
            arrow_options_from_context,
        )

        if parallel:
            arrow_loader: CollectionArrowLoader = CollectionArrowLoaderParallel(
                client,
                collection,
                incremental=incremental,
                chunk_size=chunk_size,
                connection_url=connection_url,
                partition_size_bytes=partition_size_bytes,
            )
        else:
            arrow_loader = CollectionArrowLoader(
                client, collection, incremental=incremental, chunk_size=chunk_size
            )
        yield from arrow_loader.load_documents(
            limit=limit,
            filter_=filter_,
            projection=projection,
            arrow_options=arrow_options_from_context(
                pymongoarrow_schema, on_inferred_type_mismatch
            ),
        )
    else:
        if parallel:
            loader: CollectionLoader = CollectionLoaderParallel(
                client,
                collection,
                incremental=incremental,
                chunk_size=chunk_size,
                partition_size_bytes=partition_size_bytes,
            )
        else:
            loader = CollectionLoader(
                client, collection, incremental=incremental, chunk_size=chunk_size
            )
        yield from loader.load_documents(
            limit=limit, filter_=filter_, projection=projection
        )


def convert_mongo_objs(value: Any) -> Any:
    """Converts BSON types to the types of the "python" backend"""
    if isinstance(value, (ObjectId, Decimal128)):
        return str(value)
    if isinstance(value, Regex):
        # patterns can use PCRE syntax that Python `re` does not compile
        return value.pattern
    if isinstance(value, Timestamp):
        return value.as_datetime().astimezone(timezone.utc)
    if isinstance(value, DBRef):
        return {key: convert_mongo_objs(item) for key, item in value.as_doc().items()}
    if isinstance(value, (MinKey, MaxKey)):
        return None

    return value


def _pymongoarrow_available() -> bool:
    try:
        import pymongoarrow  # type: ignore # noqa: F401
    except ImportError:
        return False
    return True


def client_from_credentials(connection_url: str) -> TMongoClient:
    with warnings.catch_warnings():
        ignore_compressor_warnings()
        client: TMongoClient = MongoClient(
            connection_url, **client_options(connection_url)
        )
    return client


def client_options(connection_url: str) -> Dict[str, Any]:
    """Keyword arguments of `MongoClient` for `connection_url`"""
    options: Dict[str, Any] = dict(
        uuidRepresentation="standard",
        tz_aware=True,
        tzinfo=timezone.utc,
        # dates outside of the python range are clamped instead of failing the load
        datetime_conversion=DatetimeConversion.DATETIME_CLAMP,
    )
    # keyword arguments override the URL, where an empty `compressors=` disables compression
    if not re.search(r"[?&]compressors=", connection_url, re.IGNORECASE):
        options["compressors"] = COMPRESSORS
    return options


def ignore_compressor_warnings() -> None:
    """pymongo skips compressors whose package is not installed, zlib is always there"""
    warnings.filterwarnings("ignore", "Wire protocol compression with")
    warnings.filterwarnings("ignore", "Unsupported compressor: $")


def _is_path_prefix(parent: str, path: str) -> bool:
    return path == parent or path.startswith(parent + ".")


def _raise_if_intersection(filter1: Dict[str, Any], filter2: Dict[str, Any]) -> None:
    """Raises when both filters use the same operator on a field"""
    field_inter = filter1.keys() & filter2.keys()
    for field in field_inter:
        operators1, operators2 = filter1[field], filter2[field]
        if not isinstance(operators1, dict) or not isinstance(operators2, dict):
            continue
        if operators1.keys() & operators2.keys():
            str_repr = str({field: operators1})
            raise ValueError(
                f"Filter operator {str_repr} is also used by the incremental. Remove it "
                "from `filter_`."
            )


@configspec
class MongoDbCollectionConfiguration(BaseConfiguration):
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]


@configspec
class MongoDbCollectionResourceConfiguration(BaseConfiguration):
    connection_url: dlt.TSecretValue = dlt.secrets.value
    database: Optional[str] = dlt.config.value
    collection: str = dlt.config.value
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value
    parallel: Optional[bool] = False
    partition_size_bytes: int = DEFAULT_PARTITION_SIZE_BYTES
    projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = dlt.config.value


__source_name__ = "mongodb"
