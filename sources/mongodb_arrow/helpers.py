from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional

import dlt
from dlt.common.typing import TDataItem

import bson

import pyarrow
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongoarrow.api import Schema, find_arrow_all

if TYPE_CHECKING:
    TMongoClient = MongoClient[Any]
    TCollection = Collection[Any]  # type: ignore
    TCursor = Cursor[Any]
else:
    TMongoClient = Any
    TCollection = Any
    TCursor = Any


def _prepare_incremental_query(incremental):
    if incremental is None:
        return {}

    field = incremental.cursor_path
    last_value = incremental.last_value

    if not last_value:
        return {}

    filt = {}
    if incremental.last_value_func is max:
        filt = {field: {"$gte": last_value}}
        if incremental.end_value:
            filt[field]["$lt"] = incremental.end_value

    elif incremental.last_value_func is min:
        filt = {field: {"$lte": last_value}}
        if incremental.end_value:
            filt[field]["$gt"] = incremental.end_value

    return filt


def _find_id_fields(collection):
    document = collection.find_one()

    ids = []
    for field, value in document.items():
        if isinstance(value, bson.ObjectId):
            ids.append(field)

    return ids


def collection_arrow_documents(
    client: TMongoClient,
    collection: TCollection,
    query: Optional[Dict[str, Any]] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    limit: Optional[int] = None,
    chunk_size: int = 10000,
) -> Iterator[TDataItem]:
    """
    A DLT source which loads data from a Mongo database using PyMongo.
    Resources are automatically created for the collection.

    Args:
        client (MongoClient): The PyMongo client `pymongo.MongoClient` instance.
        collection (Collection): The collection `pymongo.collection.Collection` to load.
        query (Optional[dict]): The query to filter the documents.
        incremental (Optional[dlt.sources.incremental[Any]]): The incremental configuration.

    Returns:
        Iterable[DltResource]: A list of DLT resources for each collection to be loaded.
    """
    query = query or {}
    inc_query = _prepare_incremental_query(incremental)

    for field, value in inc_query.items():
        if field in query:
            raise ValueError(
                (
                    f"Field {field} is present in the query. Using the"
                    " same field for incremental and query is not available."
                )
            )

        query[field] = value

    ids = _find_id_fields(collection)

    table = find_arrow_all(collection, query)
    for doc in table.to_batches(1):
        doc = doc.to_pydict()
        for field in doc:
            if field in ids:
                doc[field] = str(doc[field][0])
            else:
                doc[field] = doc[field][0]

        yield doc
