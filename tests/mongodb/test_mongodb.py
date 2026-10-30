import sys
from typing import Any, Dict, List
from unittest import mock

import dlt
import pyarrow as pa
import pymongoarrow
import pytest
from packaging.version import Version
from dlt.sources import DltResource

from sources.mongodb import mongodb, mongodb_collection

from tests.mongodb.utils import FakeClient, FakeCollection, FakeDatabase, make_documents
from tests.utils import load_table_counts

CONNECTION_URL = "mongodb://localhost:27017"
# nested documents are flattened in both formats, arrays differ: child tables or json
SCALAR_COLUMNS = [
    "_id",
    "n",
    "title",
    "updated_at",
    "price",
    "payload",
    "code",
    "ref__oid",
    "ref__x",
]


def _select(pipeline: dlt.Pipeline, table_name: str) -> List[Dict[str, Any]]:
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            f"SELECT {', '.join(SCALAR_COLUMNS)} FROM {table_name} ORDER BY n"
        )
    return [dict(zip(SCALAR_COLUMNS, row)) for row in rows]


@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_mongodb_collection_incremental(
    fake_collection: FakeCollection, data_item_format: str
) -> None:
    resource = mongodb_collection(
        connection_url=CONNECTION_URL,
        database="db",
        collection="docs",
        incremental=dlt.sources.incremental("updated_at"),
        data_item_format=data_item_format,
    )
    # returns the resource of the collection, it is not data
    assert isinstance(resource, DltResource)
    assert resource.name == "docs"

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_collection_incremental",
        destination="duckdb",
        dev_mode=True,
    )
    pipeline.run(resource)
    assert load_table_counts(pipeline, "docs") == {"docs": 10}
    columns = pipeline.default_schema.get_table_columns("docs")
    assert columns["_id"]["data_type"] == "text"
    assert columns["updated_at"]["data_type"] == "timestamp"

    # the state carries over: nothing new on the second run, then only the new documents
    pipeline.run(resource)
    assert load_table_counts(pipeline, "docs") == {"docs": 10}
    fake_collection.insert_many(make_documents(3, start=10))
    pipeline.run(resource)
    assert load_table_counts(pipeline, "docs") == {"docs": 13}


def test_mongodb_collection_arrow_matches_object(
    fake_collection: FakeCollection,
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_arrow_matches_object",
        destination="duckdb",
        dev_mode=True,
    )
    for data_item_format in ("object", "arrow"):
        resource = mongodb_collection(
            connection_url=CONNECTION_URL,
            database="db",
            collection="docs",
            data_item_format=data_item_format,
        )
        pipeline.run(resource, table_name=f"docs_{data_item_format}")

    arrow_rows, object_rows = _select(pipeline, "docs_arrow"), _select(
        pipeline, "docs_object"
    )
    if Version(pymongoarrow.__version__) < Version("1.6"):
        # older pymongoarrow loses the ObjectId type in nested documents, they load as binary
        for row in arrow_rows + object_rows:
            del row["ref__oid"]
    assert arrow_rows == object_rows


@pytest.mark.parametrize("data_item_format", ["object", "arrow"])
def test_mongodb_source(fake_collection: FakeCollection, data_item_format: str) -> None:
    source = mongodb(
        connection_url=CONNECTION_URL,
        database="db",
        data_item_format=data_item_format,
        chunk_size=4,
    )
    items = list(source.resources["docs"])
    if data_item_format == "arrow":
        assert all(isinstance(item, pa.Table) for item in items)
        assert [item.num_rows for item in items] == [4, 4, 2]
    else:
        assert len(items) == 10

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_source", destination="duckdb", dev_mode=True
    )
    pipeline.run(source)
    assert load_table_counts(pipeline, "docs", "other") == {"docs": 10, "other": 3}


def test_arrow_falls_back_to_object_without_pymongoarrow(
    fake_collection: FakeCollection,
) -> None:
    with mock.patch.dict(sys.modules, {"pymongoarrow": None}):
        items = list(
            mongodb_collection(
                connection_url=CONNECTION_URL,
                database="db",
                collection="docs",
                data_item_format="arrow",
            )
        )
    assert len(items) == 10
    assert all(isinstance(item, dict) for item in items)


def test_mongodb_source_skips_system_collections(
    fake_collection: FakeCollection, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = FakeDatabase(
        [
            fake_collection,
            FakeCollection("system.views", []),
            FakeCollection("system.buckets.ts", []),
        ]
    )
    monkeypatch.setattr(
        "sources.mongodb.client_from_credentials", lambda _: FakeClient(database)
    )

    source = mongodb(connection_url=CONNECTION_URL, database="db")
    assert list(source.resources.keys()) == ["docs"]
    # system collections are loaded when requested
    source = mongodb(
        connection_url=CONNECTION_URL, database="db", collection_names=["system.views"]
    )
    assert list(source.resources.keys()) == ["system.views"]
