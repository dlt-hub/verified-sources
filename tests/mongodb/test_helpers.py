import warnings
from datetime import datetime, timezone
from typing import Optional

import pytest
from bson import DBRef, Decimal128, MaxKey, MinKey, ObjectId, Regex, Timestamp

from sources.mongodb.helpers import (
    COMPRESSORS,
    client_from_credentials,
    client_options,
    convert_mongo_objs,
)


def test_convert_mongo_objs() -> None:
    oid = ObjectId()
    assert convert_mongo_objs(oid) == str(oid)
    assert convert_mongo_objs(Decimal128("1.10")) == "1.10"
    assert convert_mongo_objs(Regex("^foo", "i")) == "^foo"
    # PCRE syntax that python `re` does not compile
    assert convert_mongo_objs(Regex(r"(?<name>a)\p{L}", "i")) == r"(?<name>a)\p{L}"
    assert convert_mongo_objs(DBRef("users", oid, "db")) == {
        "$ref": "users",
        "$id": str(oid),
        "$db": "db",
    }
    assert convert_mongo_objs(MinKey()) is None
    assert convert_mongo_objs(MaxKey()) is None
    converted = convert_mongo_objs(Timestamp(1704067200, 1))
    assert converted == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert converted.tzinfo == timezone.utc


@pytest.mark.parametrize(
    "url,compressors",
    [
        ("mongodb+srv://user:pass@cluster.example.net/db", COMPRESSORS),
        ("mongodb://localhost/?compressors=snappy", None),
        ("mongodb://localhost/?retryWrites=true&Compressors=", None),
    ],
    ids=["default", "from_url", "disabled_in_url"],
)
def test_client_compressors(url: str, compressors: Optional[str]) -> None:
    # the compressors of the URL win, the client options would override them
    assert client_options(url).get("compressors") == compressors


def test_client_skips_missing_compressors() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        client = client_from_credentials("mongodb://localhost:1/?connect=false")
    assert "zlib" in client.options.pool_options._compression_settings.compressors
