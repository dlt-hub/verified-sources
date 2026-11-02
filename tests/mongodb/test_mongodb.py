import sys
from typing import Any
from unittest import mock

import dlt
import pyarrow as pa
import pytest
from dlt.sources import DltResource

from sources.mongodb import mongodb, mongodb_collection

from tests.mongodb.utils import FakeClient, FakeCollection, FakeDatabase
from tests.utils import load_table_counts

CONNECTION_URL = "mongodb://localhost:27017"


def test_mongodb_collection_returns_resource(fake_collection: FakeCollection) -> None:
    resource = mongodb_collection(
        connection_url=CONNECTION_URL, database="db", collection="docs"
    )

    # the resource of the collection, not its data
    assert isinstance(resource, DltResource)
    assert resource.name == "docs"
    assert len(list(resource)) == 10


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_mongodb_source(fake_collection: FakeCollection, backend: str) -> None:
    source = mongodb(
        connection_url=CONNECTION_URL,
        database="db",
        backend=backend,
        chunk_size=4,
    )
    items = list(source.resources["docs"])
    if backend == "pyarrow":
        assert all(isinstance(item, pa.Table) for item in items)
        assert [item.num_rows for item in items] == [4, 4, 2]
    else:
        assert len(items) == 10

    pipeline = dlt.pipeline(
        pipeline_name="mongodb_source", destination="duckdb", dev_mode=True
    )
    pipeline.run(source)
    assert load_table_counts(pipeline, "docs", "other") == {"docs": 10, "other": 3}


def test_pyarrow_falls_back_to_python_without_pymongoarrow(
    fake_collection: FakeCollection,
) -> None:
    with mock.patch.dict(sys.modules, {"pymongoarrow": None}):
        items = list(
            mongodb_collection(
                connection_url=CONNECTION_URL,
                database="db",
                collection="docs",
                backend="pyarrow",
            )
        )
    assert len(items) == 10
    assert all(isinstance(item, dict) for item in items)


@pytest.mark.parametrize(
    "data_item_format,item_type", [("object", dict), ("arrow", pa.Table)]
)
def test_data_item_format_selects_backend(
    fake_collection: FakeCollection, data_item_format: str, item_type: type, mocker: Any
) -> None:
    warning = mocker.patch("dlt.common.logger.warning")
    items = list(
        mongodb_collection(
            connection_url=CONNECTION_URL,
            database="db",
            collection="docs",
            data_item_format=data_item_format,
        )
    )

    assert items and all(isinstance(item, item_type) for item in items)
    assert "`data_item_format` is deprecated" in str(warning.call_args_list)


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
