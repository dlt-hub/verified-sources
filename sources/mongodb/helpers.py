"""Mongo database source helpers"""

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
from dlt.common.data_writers import TDataItemFormat
from dlt.common.typing import TDataItem
from dlt.common.utils import map_nested_values_in_place
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor


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

TTypeMismatch = Literal["raise", "warn", "ignore"]


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
        """Build a filtering operator from the incremental range.

        Returns:
            Dict[str, Any]: A dictionary with the filter operator.
        """
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
        """Combine the incremental filter with the user filter.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.

        Returns:
            Dict[str, Any]: The filter to query the collection with.
        """
        filter_op = self._filter_op
        _raise_if_intersection(filter_op, filter_)
        if filter_op and filter_:
            return {"$and": [filter_op, filter_]}
        return filter_op or filter_

    def _projection_op(
        self, projection: Optional[Union[Mapping[str, Any], Iterable[str]]]
    ) -> Optional[Dict[str, Any]]:
        """Build a projection operator.

        Args:
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): A tuple of fields to include or a dict specifying fields to include or exclude.
            The incremental cursor field is always loaded.

        Returns:
            Optional[Dict[str, Any]]: A dictionary with the projection operator.
        """
        if projection is None:
            return None

        if isinstance(projection, Mapping):
            projection_dict = dict(projection)
        else:
            projection_dict = {field: 1 for field in projection}

        if self.incremental:
            cursor_path = self.incremental.cursor_path
            # `_id` may be excluded in an inclusion projection and included in an exclusion one
            if any(
                value == 1 for field, value in projection_dict.items() if field != "_id"
            ):
                projection_dict[cursor_path] = 1
            elif projection_dict.pop(cursor_path, None) is not None:
                logger.warning(
                    f"Cursor field `{cursor_path}` was removed from exclusion projection"
                )

        return projection_dict

    def _limit(self, cursor: TCursor, limit: Optional[int] = None) -> TCursor:
        """Apply a limit to the cursor, if needed.

        Args:
            cursor (Cursor): The cursor to apply the limit.
            limit (Optional[int]): The number of documents to load.

        Returns:
            Cursor: The cursor with the limit applied (if given).
        """
        if limit not in (0, None):
            if not self._sort_op:
                logger.warning(
                    "Using limit without ordering - results may be inconsistent."
                )

            cursor = cursor.limit(abs(limit))

        return cursor

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> Iterator[TDataItem]:
        """Construct the query and load the documents from the collection.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            limit (Optional[int]): The number of documents to load.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.

        Yields:
            Iterator[TDataItem]: An iterator of the loaded documents.
        """
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
    @property
    def _batches_sort_op(self) -> List[Tuple[str, int]]:
        """Sort with a unique field, so skip and limit split the collection without gaps or overlaps"""
        sort_op = self._sort_op
        if not any(field == "_id" for field, _ in sort_op):
            sort_op.append(("_id", ASCENDING))
        return sort_op

    def _get_document_count(self, filter_: Dict[str, Any]) -> int:
        return self.collection.count_documents(filter=self._query_filter(filter_))

    def _create_batches(
        self, filter_: Dict[str, Any], limit: Optional[int] = None
    ) -> List[Dict[str, int]]:
        doc_count = self._get_document_count(filter_)
        if limit:
            doc_count = min(doc_count, abs(limit))

        batches = []
        left_to_load = doc_count

        for sk in range(0, doc_count, self.chunk_size):
            batches.append(dict(skip=sk, limit=min(self.chunk_size, left_to_load)))
            left_to_load -= self.chunk_size

        return batches

    def _get_cursor(
        self,
        filter_: Dict[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> TCursor:
        """Get a reading cursor for the collection.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.

        Returns:
            Cursor: The cursor for the collection.
        """
        cursor = self.collection.find(
            filter=self._query_filter(filter_),
            projection=self._projection_op(projection),
        )
        return cursor.sort(self._batches_sort_op)  # type: ignore[no-any-return]

    @dlt.defer
    def _run_batch(self, cursor: TCursor, batch: Dict[str, int]) -> TDataItem:
        cursor = cursor.clone()

        data = []
        for document in cursor.skip(batch["skip"]).limit(batch["limit"]):
            data.append(map_nested_values_in_place(convert_mongo_objs, document))

        return data

    def _get_all_batches(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> Iterator[TDataItem]:
        """Load all documents from the collection in parallel batches.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            limit (Optional[int]): The maximum number of documents to load.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.

        Yields:
            Iterator[TDataItem]: An iterator of the loaded documents.
        """
        batches = self._create_batches(filter_=filter_, limit=limit)
        cursor = self._get_cursor(filter_=filter_, projection=projection)

        for batch in batches:
            yield self._run_batch(cursor=cursor, batch=batch)

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> Iterator[TDataItem]:
        """Load documents from the collection in parallel.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            limit (Optional[int]): The number of documents to load.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.

        Yields:
            Iterator[TDataItem]: An iterator of the loaded documents.
        """
        for document in self._get_all_batches(
            limit=limit, filter_=filter_, projection=projection
        ):
            yield document


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
    data_item_format: Optional[TDataItemFormat] = "object",
    on_inferred_type_mismatch: TTypeMismatch = "ignore",
) -> Iterator[TDataItem]:
    """
    A DLT source which loads data from a Mongo database using PyMongo.
    Resources are automatically created for the collection.

    Args:
        client (MongoClient): The PyMongo client `pymongo.MongoClient` instance.
        collection (Collection): The collection `pymongo.collection.Collection` to load.
        filter_ (Dict[str, Any]): The filter to apply to the collection.
        projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.
            when loading the collection. Supported inputs:
                include (list) - ["year", "title"]
                include (dict) - {"year": True, "title": True}
                exclude (dict) - {"released": False, "runtime": False}
            Note: Can't mix include and exclude statements '{"title": True, "released": False}`
        pymongoarrow_schema (pymongoarrow.schema.Schema): The mapping of field types to convert BSON to Arrow.
        incremental (Optional[dlt.sources.incremental[Any]]): The incremental configuration.
        parallel (bool): Option to enable parallel loading for the collection. Default is False.
        limit (Optional[int]): The maximum number of documents to load.
        chunk_size (Optional[int]): The number of documents to load in each batch.
        data_item_format (Optional[TDataItemFormat]): The data format to use for loading.
            Supported formats:
                object - Python objects (dicts, lists).
                arrow - Apache Arrow tables.
        on_inferred_type_mismatch (TTypeMismatch): Applies when `pymongoarrow_schema` is not passed. The
            Arrow format then infers the type of a field from its first value in a batch and loads
            values of other types as null: "ignore" (default, fastest) does that silently, "warn" and
            "raise" detect such values with a second parse of each batch. Values that do not match
            `pymongoarrow_schema` always raise `FieldTypeMismatchError`.

    Yields:
        Iterator[TDataItem]: Lists of documents or Arrow tables.
    """
    if data_item_format == "arrow" and not _pymongoarrow_available():
        logger.warning(
            "'pymongoarrow' is not installed; falling back to standard MongoDB CollectionLoader."
        )
        data_item_format = "object"

    if data_item_format != "arrow" and pymongoarrow_schema:
        logger.warning(
            "Received value for `pymongoarrow_schema`, but `data_item_format=='object'` "
            "Use `data_item_format=='arrow'` to enforce schema."
        )

    if data_item_format == "arrow":
        from .arrow import (
            CollectionArrowLoader,
            CollectionArrowLoaderParallel,
            arrow_options_from_context,
        )

        arrow_loader_class = (
            CollectionArrowLoaderParallel if parallel else CollectionArrowLoader
        )
        arrow_loader = arrow_loader_class(
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
        loader_class = CollectionLoaderParallel if parallel else CollectionLoader
        loader = loader_class(
            client, collection, incremental=incremental, chunk_size=chunk_size
        )
        yield from loader.load_documents(
            limit=limit, filter_=filter_, projection=projection
        )


def convert_mongo_objs(value: Any) -> Any:
    """MongoDB to dlt type conversion when using Python loaders."""
    if isinstance(value, (ObjectId, Decimal128)):
        return str(value)
    if isinstance(value, Regex):
        # patterns may use PCRE syntax that Python `re` does not compile
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
    client: TMongoClient = MongoClient(
        connection_url,
        uuidRepresentation="standard",
        tz_aware=True,
        tzinfo=timezone.utc,
        # dates outside of the python range are clamped instead of failing the load
        datetime_conversion=DatetimeConversion.DATETIME_CLAMP,
    )
    return client


def _raise_if_intersection(filter1: Dict[str, Any], filter2: Dict[str, Any]) -> None:
    """
    Raise an exception, if the given filters'
    fields are intersecting.

    Args:
        filter1 (Dict[str, Any]): The first filter.
        filter2 (Dict[str, Any]): The second filter.
    """
    field_inter = filter1.keys() & filter2.keys()
    for field in field_inter:
        operators1, operators2 = filter1[field], filter2[field]
        if not isinstance(operators1, dict) or not isinstance(operators2, dict):
            continue
        if operators1.keys() & operators2.keys():
            str_repr = str({field: operators1})
            raise ValueError(
                (
                    f"Filtering operator {str_repr} is already used by the "
                    "incremental and can't be used in the filter."
                )
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
    projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = dlt.config.value


__source_name__ = "mongodb"
