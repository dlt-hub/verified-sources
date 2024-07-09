"""Mongo database source helpers"""

from itertools import islice
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

import dlt
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.timestamp import Timestamp
from dlt.common import logger
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.data_writers import TDataItemFormat
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem
from dlt.common.utils import map_nested_in_place
from pendulum import _datetime
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor


if TYPE_CHECKING:
    TMongoClient = MongoClient[Any]
    TCollection = Collection[Any]  # type: ignore
    TCursor = Cursor[Any]
else:
    TMongoClient = Any
    TCollection = Any
    TCursor = Any


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

        if incremental:
            self.cursor_field = incremental.cursor_path
            self.last_value = incremental.last_value
        else:
            self.cursor_column = None
            self.last_value = None

    @property
    def _sort_op(self) -> List[Optional[Tuple[str, int]]]:
        if not self.incremental or not self.last_value:
            return []

        if (
            self.incremental.row_order == "asc"
            and self.incremental.last_value_func is max
        ) or (
            self.incremental.row_order == "desc"
            and self.incremental.last_value_func is min
        ):
            return [(self.cursor_field, ASCENDING)]

        elif (
            self.incremental.row_order == "asc"
            and self.incremental.last_value_func is min
        ) or (
            self.incremental.row_order == "desc"
            and self.incremental.last_value_func is max
        ):
            return [(self.cursor_field, DESCENDING)]

        return []

    @property
    def _filter_op(self) -> Dict[str, Any]:
        """Build a filtering operator.

        Includes a field and the filtering condition for it.

        Returns:
            Dict[str, Any]: A dictionary with the filter operator.
        """
        if not (self.incremental and self.last_value):
            return {}

        filt = {}
        if self.incremental.last_value_func is max:
            filt = {self.cursor_field: {"$gte": self.last_value}}
            if self.incremental.end_value:
                filt[self.cursor_field]["$lt"] = self.incremental.end_value

        elif self.incremental.last_value_func is min:
            filt = {self.cursor_field: {"$lte": self.last_value}}
            if self.incremental.end_value:
                filt[self.cursor_field]["$gt"] = self.incremental.end_value

        return filt

    def _limit(self, cursor: Cursor, limit: Optional[int] = None) -> Cursor:  # type: ignore
        """Apply a limit to the cursor, if needed.

        Args:
            cursor (Cursor): The cursor to apply the limit.
            limit (Optional[int]): The number of documents to load.

        Returns:
            Cursor: The cursor with the limit applied (if given).
        """
        if limit not in (0, None):
            if self.incremental is None or self.incremental.last_value_func is None:
                logger.warning(
                    "Using limit without ordering - results may be inconsistent."
                )

            cursor = cursor.limit(abs(limit))

        return cursor

    def load_documents(self, limit: Optional[int] = None) -> Iterator[TDataItem]:
        """Construct the query and load the documents from the collection.

        Args:
            limit (Optional[int]): The number of documents to load.

        Yields:
            Iterator[TDataItem]: An iterator of the loaded documents.
        """
        cursor = self.collection.find(self._filter_op)
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)

        cursor = self._limit(cursor, limit)

        while docs_slice := list(islice(cursor, self.chunk_size)):
            yield map_nested_in_place(convert_mongo_objs, docs_slice)


class CollectionLoaderParallel(CollectionLoader):
    def _get_document_count(self) -> int:
        return self.collection.count_documents(filter=self._filter_op)

    def _create_batches(self, limit: Optional[int] = None) -> List[Dict[str, int]]:
        doc_count = self._get_document_count()
        if limit:
            doc_count = min(doc_count, abs(limit))

        batches = []
        left_to_load = doc_count

        for sk in range(0, doc_count, self.chunk_size):
            batches.append(dict(skip=sk, limit=min(self.chunk_size, left_to_load)))
            left_to_load -= self.chunk_size

        return batches

    def _get_cursor(self) -> TCursor:
        cursor = self.collection.find(filter=self._filter_op)
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)

        return cursor

    @dlt.defer
    def _run_batch(self, cursor: TCursor, batch: Dict[str, int]) -> TDataItem:
        cursor = cursor.clone()

        data = []
        for document in cursor.skip(batch["skip"]).limit(batch["limit"]):
            data.append(map_nested_in_place(convert_mongo_objs, document))

        return data

    def _get_all_batches(self, limit: Optional[int] = None) -> Iterator[TDataItem]:
        """Load all documents from the collection in parallel batches.

        Args:
            limit (Optional[int]): The maximum number of documents to load.

        Yields:
            Iterator[TDataItem]: An iterator of the loaded documents.
        """
        batches = self._create_batches(limit)
        cursor = self._get_cursor()

        for batch in batches:
            yield self._run_batch(cursor=cursor, batch=batch)

    def load_documents(self, limit: Optional[int] = None) -> Iterator[TDataItem]:
        """Load documents from the collection in parallel.

        Args:
            limit (Optional[int]): The number of documents to load.

        Yields:
            Iterator[TDataItem]: An iterator of the loaded documents.
        """
        for document in self._get_all_batches(limit):
            yield document


class CollectionArrowLoader(CollectionLoader):
    """
    Mongo DB collection loader, which uses
    Apache Arrow for data processing.
    """

    def load_documents(self, limit: Optional[int] = None) -> Iterator[Any]:
        """
        Load documents from the collection in Apache Arrow format.

        Args:
            limit (Optional[int]): The number of documents to load.

        Yields:
            Iterator[Any]: An iterator of the loaded documents.
        """
        from pymongoarrow.context import PyMongoArrowContext  # type: ignore
        from pymongoarrow.lib import process_bson_stream  # type: ignore

        context = PyMongoArrowContext.from_schema(
            None, codec_options=self.collection.codec_options
        )

        cursor = self.collection.find_raw_batches(
            self._filter_op, batch_size=self.chunk_size
        )
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)  # type: ignore

        cursor = self._limit(cursor, limit)  # type: ignore

        for batch in cursor:
            process_bson_stream(batch, context)

            table = context.finish()
            yield convert_arrow_columns(table)


class CollectionArrowLoaderParallel(CollectionLoaderParallel):
    """
    Mongo DB collection parallel loader, which uses
    Apache Arrow for data processing.
    """

    def _get_cursor(self) -> TCursor:
        cursor = self.collection.find_raw_batches(
            filter=self._filter_op, batch_size=self.chunk_size
        )
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)  # type: ignore

        return cursor

    @dlt.defer
    def _run_batch(self, cursor: TCursor, batch: Dict[str, int]) -> TDataItem:
        from pymongoarrow.context import PyMongoArrowContext
        from pymongoarrow.lib import process_bson_stream

        cursor = cursor.clone()

        context = PyMongoArrowContext.from_schema(
            None, codec_options=self.collection.codec_options
        )

        for chunk in cursor.skip(batch["skip"]).limit(batch["limit"]):
            process_bson_stream(chunk, context)

            table = context.finish()
            yield convert_arrow_columns(table)


def collection_documents(
    client: TMongoClient,
    collection: TCollection,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    parallel: bool = False,
    limit: Optional[int] = None,
    chunk_size: Optional[int] = 10000,
    data_item_format: Optional[TDataItemFormat] = "object",
) -> Iterator[TDataItem]:
    """
    A DLT source which loads data from a Mongo database using PyMongo.
    Resources are automatically created for the collection.

    Args:
        client (MongoClient): The PyMongo client `pymongo.MongoClient` instance.
        collection (Collection): The collection `pymongo.collection.Collection` to load.
        incremental (Optional[dlt.sources.incremental[Any]]): The incremental configuration.
        parallel (bool): Option to enable parallel loading for the collection. Default is False.
        limit (Optional[int]): The maximum number of documents to load.
        chunk_size (Optional[int]): The number of documents to load in each batch.
        data_item_format (Optional[TDataItemFormat]): The data format to use for loading.
            Supported formats:
                object - Python objects (dicts, lists).
                arrow - Apache Arrow tables.

    Returns:
        Iterable[DltResource]: A list of DLT resources for each collection to be loaded.
    """
    if parallel:
        if data_item_format == "arrow":
            LoaderClass = CollectionArrowLoaderParallel
        elif data_item_format == "object":
            LoaderClass = CollectionLoaderParallel  # type: ignore
    else:
        if data_item_format == "arrow":
            LoaderClass = CollectionArrowLoader  # type: ignore
        elif data_item_format == "object":
            LoaderClass = CollectionLoader  # type: ignore

    loader = LoaderClass(
        client, collection, incremental=incremental, chunk_size=chunk_size
    )
    for data in loader.load_documents(limit=limit):
        yield data


def convert_mongo_objs(value: Any) -> Any:
    if isinstance(value, (ObjectId, Decimal128)):
        return str(value)
    if isinstance(value, _datetime.datetime):
        return ensure_pendulum_datetime(value)
    if isinstance(value, Regex):
        return value.try_compile().pattern
    if isinstance(value, Timestamp):
        date = value.as_datetime()
        return ensure_pendulum_datetime(date)

    return value


def convert_arrow_columns(table: Any) -> Any:
    """Convert the given table columns to Python types.

    Args:
        table (pyarrow.lib.Table): The table to convert.

    Returns:
        pyarrow.lib.Table: The table with the columns converted.
    """
    from pymongoarrow.types import _is_binary, _is_code, _is_decimal128, _is_objectid  # type: ignore
    from dlt.common.libs.pyarrow import pyarrow

    for i, field in enumerate(table.schema):
        if _is_objectid(field.type) or _is_decimal128(field.type):
            col_values = [str(value) for value in table[field.name]]
            table = table.set_column(
                i,
                pyarrow.field(field.name, pyarrow.string()),
                pyarrow.array(col_values, type=pyarrow.string()),
            )
        else:
            type_ = None
            if _is_binary(field.type):
                type_ = pyarrow.binary()
            elif _is_code(field.type):
                type_ = pyarrow.string()

            if type_:
                col_values = [value.as_py() for value in table[field.name]]
                table = table.set_column(
                    i,
                    pyarrow.field(field.name, type_),
                    pyarrow.array(col_values, type=type_),
                )
    return table


def client_from_credentials(connection_url: str) -> TMongoClient:
    client: TMongoClient = MongoClient(
        connection_url, uuidRepresentation="standard", tz_aware=True
    )
    return client


@configspec
class MongoDbCollectionConfiguration(BaseConfiguration):
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]


@configspec
class MongoDbCollectionResourceConfiguration(BaseConfiguration):
    connection_url: str = dlt.secrets.value
    database: Optional[str] = dlt.config.value
    collection: str = dlt.config.value
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value
    parallel: Optional[bool] = False


__source_name__ = "mongodb"
