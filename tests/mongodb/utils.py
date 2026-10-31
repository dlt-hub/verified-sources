import math
import random
import subprocess
import uuid
from copy import copy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import bson
import dlt
import pytest
from bson import (
    Binary,
    Code,
    Decimal128,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    Regex,
    Timestamp,
)
from bson.codec_options import CodecOptions
from pymongo.errors import OperationFailure

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
        **bounds: Any,
    ) -> None:
        self.collection = collection
        self.filter = filter_
        self.projection = projection
        self.raw_batch_size = raw_batch_size
        self.hint = bounds.get("hint")
        self.min = bounds.get("min")
        self.max = bounds.get("max")
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
        self.insert_many(documents)

    def insert_many(self, documents: Sequence[Dict[str, Any]]) -> None:
        self.documents.extend(bson.encode(document) for document in documents)

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,  # noqa: A002
        projection: Optional[Dict[str, Any]] = None,
        **bounds: Any,
    ) -> FakeCursor:
        cursor = FakeCursor(self, filter, projection, **bounds)
        self.cursors.append(cursor)
        return cursor

    def find_raw_batches(
        self,
        filter: Optional[Dict[str, Any]] = None,  # noqa: A002
        batch_size: int = 0,
        projection: Optional[Dict[str, Any]] = None,
        **bounds: Any,
    ) -> FakeCursor:
        cursor = FakeCursor(
            self, filter, projection, raw_batch_size=batch_size or 101, **bounds
        )
        self.cursors.append(cursor)
        return cursor

    def estimated_document_count(self) -> int:
        return len(self.documents)

    def count_documents(self, filter: Dict[str, Any]) -> int:  # noqa: A002
        return len(self.documents)

    def aggregate(self, pipeline: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        raise OperationFailure(f"the fake server runs no aggregations: {pipeline}")

    def options(self) -> Dict[str, Any]:
        return {}


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


def make_seeded_documents(count: int) -> List[Dict[str, Any]]:
    """Documents updated at random, unique seconds within 90 days, not in insert order"""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    documents = make_documents(count)
    for document, seconds in zip(
        documents, random.sample(range(90 * 24 * 3600), count)
    ):
        document["updated_at"] = base + timedelta(seconds=seconds)
    return documents


def make_mixed_id_documents(majority: str, count: int) -> List[Dict[str, Any]]:
    """`count` documents with `_id` of the `majority` type in random order and one document with
    an `_id` of every other type, numbered by `n`"""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    majority_ids: Dict[str, Any] = {
        "objectId": lambda n: ObjectId(),
        # ints, longs, doubles and decimals are one type in `_id` ranges
        "number": lambda n: [n, Int64(-n - 1), n + 0.5, Decimal128(f"-{n}.25")][n % 4],
        "binData": lambda n: uuid.uuid4(),
        "date": lambda n: base + timedelta(minutes=n),
        "string": lambda n: f"{'Doc' if n % 2 else 'doc'}-{uuid.uuid4().hex}",
    }
    other_ids = [
        MinKey(),
        None,
        7.25,
        math.nan,
        float("-inf"),
        "other",
        {"compound": 1, "key": "a"},
        Binary(b"other", 0),
        ObjectId(),
        True,
        base - timedelta(days=1),
        Timestamp(1, 1),
        Code("function() {}"),
        MaxKey(),
    ]
    documents = [{"_id": majority_ids[majority](n), "n": n} for n in range(count)]
    documents += [{"_id": id_, "n": count + i} for i, id_ in enumerate(other_ids)]
    random.shuffle(documents)
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


class ProcessRecorder:
    """Records the processes started with `subprocess.Popen` and the most that ran at once"""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.processes: List[Any] = []
        self.most_running = 0
        popen = subprocess.Popen

        def record(*args: Any, **kwargs: Any) -> Any:
            process = popen(*args, **kwargs)
            self.processes.append(process)
            self.most_running = max(self.most_running, self.running)
            return process

        monkeypatch.setattr(subprocess, "Popen", record)

    @property
    def running(self) -> int:
        return sum(process.poll() is None for process in self.processes)


def server_reads(database: Any, collection: str) -> List[Dict[str, Any]]:
    """Queries and get mores the server ran on `collection`, with the filters and the number of
    documents it returned. Needs `profiled_mongo`."""
    return list(
        database["system.profile"].find(
            {"ns": f"{database.name}.{collection}", "op": {"$in": ["query", "getmore"]}}
        )
    )
