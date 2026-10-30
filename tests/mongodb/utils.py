from copy import copy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import bson
import dlt
from bson import Binary, Code, Decimal128, ObjectId, Regex, Timestamp
from bson.codec_options import CodecOptions

from sources.mongodb.helpers import (
    client_from_credentials,
    collection_documents,
    convert_mongo_objs,
)

CODEC_OPTIONS: CodecOptions[Any] = client_from_credentials(
    "mongodb://localhost:1/?connect=false"
).codec_options


class FakeCursor:
    """Cursor over BSON encoded documents that records the query. Yields decoded documents or,
    with `raw_batch_size`, raw BSON batches like `find_raw_batches`. Does not apply filters.
    """

    def __init__(
        self,
        collection: "FakeCollection",
        filter_: Optional[Dict[str, Any]],
        projection: Optional[Dict[str, Any]],
        raw_batch_size: Optional[int] = None,
    ) -> None:
        self.collection = collection
        self.filter = filter_
        self.projection = projection
        self.raw_batch_size = raw_batch_size
        self.sort_spec: List[Tuple[str, int]] = []
        self.skip_count = 0
        self.limit_count = 0
        self._results: Optional[Iterator[Union[Dict[str, Any], bytes]]] = None

    def sort(self, sort_spec: List[Tuple[str, int]]) -> "FakeCursor":
        self.sort_spec = list(sort_spec)
        return self

    def skip(self, count: int) -> "FakeCursor":
        self.skip_count = count
        return self

    def limit(self, count: int) -> "FakeCursor":
        self.limit_count = count
        return self

    def clone(self) -> "FakeCursor":
        cursor = copy(self)
        self.collection.cursors.append(cursor)
        return cursor

    def __iter__(self) -> "FakeCursor":
        return self

    def __next__(self) -> Union[Dict[str, Any], bytes]:
        if self._results is None:
            self._results = self._execute()
        return next(self._results)

    def _execute(self) -> Iterator[Union[Dict[str, Any], bytes]]:
        documents = list(self.collection.documents)
        for field, direction in reversed(self.sort_spec):
            documents.sort(
                key=lambda doc: bson.decode(doc)[field], reverse=direction < 0
            )
        documents = documents[self.skip_count :]
        if self.limit_count:
            documents = documents[: self.limit_count]

        if self.raw_batch_size is None:
            for document in documents:
                yield bson.decode(document, CODEC_OPTIONS)
        else:
            for start in range(0, len(documents), self.raw_batch_size):
                yield b"".join(documents[start : start + self.raw_batch_size])


class FakeCollection:
    def __init__(self, name: str, documents: Sequence[Dict[str, Any]]) -> None:
        self.name = name
        self.documents: List[bytes] = []
        self.codec_options = CODEC_OPTIONS
        self.cursors: List[FakeCursor] = []
        self.counted_filters: List[Dict[str, Any]] = []
        self.insert_many(documents)

    def insert_many(self, documents: Sequence[Dict[str, Any]]) -> None:
        self.documents.extend(bson.encode(document) for document in documents)

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,  # noqa: A002
        projection: Optional[Dict[str, Any]] = None,
    ) -> FakeCursor:
        cursor = FakeCursor(self, filter, projection)
        self.cursors.append(cursor)
        return cursor

    def find_raw_batches(
        self,
        filter: Optional[Dict[str, Any]] = None,  # noqa: A002
        batch_size: int = 0,
        projection: Optional[Dict[str, Any]] = None,
    ) -> FakeCursor:
        cursor = FakeCursor(self, filter, projection, raw_batch_size=batch_size or 101)
        self.cursors.append(cursor)
        return cursor

    def count_documents(self, filter: Dict[str, Any]) -> int:  # noqa: A002
        self.counted_filters.append(filter)
        return len(self.documents)


class FakeDatabase:
    def __init__(self, collections: Sequence[FakeCollection]) -> None:
        self.collections = {collection.name: collection for collection in collections}

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections[name]

    def list_collection_names(self) -> List[str]:
        return list(self.collections)


class FakeClient:
    def __init__(self, database: FakeDatabase) -> None:
        self.database = database

    def __getitem__(self, name: str) -> FakeDatabase:
        return self.database

    def get_default_database(self) -> FakeDatabase:
        return self.database


def make_documents(count: int, start: int = 0) -> List[Dict[str, Any]]:
    """Documents with every BSON type the source converts, nested ObjectIds and missing fields"""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    documents = []
    for n in range(start, start + count):
        document: Dict[str, Any] = {
            "_id": ObjectId(),
            "n": n,
            "title": f"title {n}",
            "updated_at": base + timedelta(days=n),
            "price": Decimal128(f"{n}.25"),
            "payload": Binary(f"payload {n}".encode()),
            "code": Code("function() { return 1; }"),
            "ref": {"oid": ObjectId(), "x": n},
            "refs": [ObjectId(), ObjectId()],
        }
        # every other document misses optional fields
        if n % 2:
            document["regex"] = Regex("^foo", "i")
            document["ts"] = Timestamp(int((base + timedelta(days=n)).timestamp()), 1)
        else:
            del document["price"]
        documents.append(document)
    return documents


def decode_documents(batch: bytes) -> List[Dict[str, Any]]:
    """Documents of a raw BSON batch as the object loader yields them"""
    return [
        {key: convert_mongo_objs(value) for key, value in document.items()}
        for document in bson.decode_all(batch, CODEC_OPTIONS)
    ]


def extract(collection: FakeCollection, **kwargs: Any) -> List[Any]:
    """Items that `collection_documents` yields for `collection` in a resource"""
    resource = dlt.resource(
        collection_documents,
        name=collection.name,
        max_table_nesting=kwargs.pop("max_table_nesting", None),
        columns=kwargs.pop("columns", None),
    )
    return list(
        resource(
            None,
            collection,
            filter_=kwargs.pop("filter_", {}),
            projection=kwargs.pop("projection", None),
            pymongoarrow_schema=kwargs.pop("pymongoarrow_schema", None),
            **kwargs,
        )
    )
