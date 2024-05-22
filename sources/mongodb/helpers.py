"""Mongo database source helpers"""

from itertools import islice
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

import dlt
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from dlt.common.configuration.specs import BaseConfiguration, configspec
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

CHUNK_SIZE = 10000


class CollectionLoader:
    def __init__(
        self,
        client: TMongoClient,
        collection: TCollection,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> None:
        self.client = client
        self.collection = collection
        self.incremental = incremental

        if incremental:
            self.cursor_field = incremental.cursor_path
        else:
            self.cursor_field = None

    @property
    def _filter_op(self) -> Dict[str, Any]:
        """Build a filter operator.

        Describes what logical statement to apply to which field.

        Returns:
            Dict[str, Any]: The filter operator.
        """
        if not self.incremental:
            return {}

        if self.incremental.start_value:
            return {self.cursor_field: {"$gte": self.incremental.start_value}}

        if self.incremental.end_value:
            return {self.cursor_field: {"$lte": self.incremental.end_value}}

        return {}

    @property
    def _sort_op(self) -> List[Optional[Tuple[str, int]]]:
        """Build a sorting operator.

        Describes what sorting to apply to which field.

        Returns:
            List[Optional[Tuple[str, int]]]: The sorting operator.
        """
        if not (self.incremental and self.incremental.row_order):
            return []

        if self.incremental.row_order == "asc":
            return [(self.cursor_field, ASCENDING)]

        if self.incremental.row_order == "desc":
            return [(self.cursor_field, DESCENDING)]

        return []

    def load_documents(self) -> Iterator[TDataItem]:
        cursor = self.collection.find(self._filter_op)
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)

        while docs_slice := list(islice(cursor, CHUNK_SIZE)):
            yield map_nested_in_place(convert_mongo_objs, docs_slice)


class CollectionLoaderParallell(CollectionLoader):
    def _get_document_count(self) -> int:
        return self.collection.count_documents(filter=self._filter_op)

    def _create_batches(self) -> List[Dict[str, int]]:
        doc_count = self._get_document_count()
        return [
            dict(skip=sk, limit=CHUNK_SIZE) for sk in range(0, doc_count, CHUNK_SIZE)
        ]

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

    def _get_all_batches(self) -> Iterator[TDataItem]:
        batches = self._create_batches()
        cursor = self._get_cursor()

        for batch in batches:
            yield self._run_batch(cursor=cursor, batch=batch)

    def load_documents(self) -> Iterator[TDataItem]:
        for document in self._get_all_batches():
            yield document


def collection_documents(
    client: TMongoClient,
    collection: TCollection,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    parallel: bool = False,
) -> Iterator[TDataItem]:
    """
    A DLT source which loads data from a Mongo database using PyMongo.
    Resources are automatically created for the collection.

    Args:
        client (MongoClient): The PyMongo client `pymongo.MongoClient` instance.
        collection (Collection): The collection `pymongo.collection.Collection` to load.
        incremental (Optional[dlt.sources.incremental[Any]]): The incremental configuration.
        parallel (bool): Option to enable parallel loading for the collection. Default is False.

    Returns:
        Iterable[DltResource]: A list of DLT resources for each collection to be loaded.
    """
    LoaderClass = CollectionLoaderParallell if parallel else CollectionLoader

    loader = LoaderClass(client, collection, incremental=incremental)
    for data in loader.load_documents():
        yield data


def convert_mongo_objs(value: Any) -> Any:
    if isinstance(value, (ObjectId, Decimal128)):
        return str(value)
    if isinstance(value, _datetime.datetime):
        return ensure_pendulum_datetime(value)
    return value


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
