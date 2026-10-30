from typing import Any, Dict, Iterator, List
from uuid import uuid4

import pytest
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError

from sources.mongodb import helpers

from tests.mongodb.utils import (
    FakeClient,
    FakeCollection,
    FakeDatabase,
    make_documents,
    make_seeded_documents,
)

LOCAL_MONGO_URL = "mongodb://localhost:27017"


@pytest.fixture
def fake_collection(monkeypatch: pytest.MonkeyPatch) -> FakeCollection:
    """A collection of 10 documents served by the client of the source"""
    collection = FakeCollection("docs", make_documents(10))
    client = FakeClient(
        FakeDatabase([collection, FakeCollection("other", make_documents(3))])
    )
    monkeypatch.setattr("sources.mongodb.client_from_credentials", lambda _: client)
    return collection


@pytest.fixture
def local_mongo() -> Iterator["Database[Any]"]:
    """A database with seeded collections on the local MongoDB of tests/mongodb/docker-compose.yml"""
    client = helpers.client_from_credentials(
        f"{LOCAL_MONGO_URL}/?serverSelectionTimeoutMS=2000"
    )
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as ex:
        raise RuntimeError(
            f"MongoDB is not running at {LOCAL_MONGO_URL}, start it with "
            "`docker compose -f tests/mongodb/docker-compose.yml up -d`"
        ) from ex
    database = client[f"dlt_test_{uuid4().hex[:8]}"]
    database["docs"].insert_many(make_documents(25))
    try:
        yield database
    finally:
        client.drop_database(database.name)
        client.close()


@pytest.fixture
def seeded_documents(local_mongo: "Database[Any]") -> List[Dict[str, Any]]:
    """Documents seeded into the `seeded` collection of `local_mongo`"""
    documents = make_seeded_documents(1050)
    # `insert_many` adds `_id` to the documents
    local_mongo["seeded"].insert_many(documents)
    return documents
