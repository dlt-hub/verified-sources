"""Mongo database source helpers"""

from itertools import islice
from typing import TYPE_CHECKING, Any, Iterator, Optional

import dlt
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem
from dlt.common.utils import map_nested_in_place
from pendulum import _datetime
from pymongo import MongoClient
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
            self.last_value = incremental.last_value
        else:
            self.cursor_column = None
            self.last_value = None

    def make_query(self) -> TCursor:
        if not self.incremental or not self.last_value:
            filter_op = {}
        elif (
            self.incremental.last_value_func is max
        ):  # Filtered according to last_value function
            filter_op = {self.cursor_field: {"$gte": self.last_value}}
        elif self.incremental.last_value_func is min:
            filter_op = {self.cursor_field: {"$lt": self.last_value}}
        else:  # Custom last_value, load everything and let incremental handle filtering
            filter_op = {}
        cursor = self.collection.find(filter_op)
        return cursor

    def load_documents(self) -> Iterator[TDataItem]:
        cursor = self.make_query()
        for document in cursor:
            yield map_nested_in_place(convert_mongo_objs, document)


def collection_documents(
    client: TMongoClient,
    collection: TCollection,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
) -> Iterator[TDataItem]:
    """
    A DLT source which loads data from a Mongo database using PyMongo.
    Resources are automatically created for the collection.

    Args:
        client (MongoClient): The PyMongo client `pymongo.MongoClient` instance.
        collection (Collection): The collection `pymongo.collection.Collection` to load.
        incremental: Optional[dlt.sources.incremental[Any]] : The incremental configuration.

    Returns:
        Iterable[DltResource]: A list of DLT resources for each collection to be loaded.
    """
    loader = CollectionLoader(client, collection, incremental=incremental)
    documents_load = loader.load_documents()
    while docs_slice := list(islice(documents_load, CHUNK_SIZE)):
        yield docs_slice


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
    connection_url: str
    database: Optional[str]
    collection: str
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]


__source_name__ = "mongodb"
