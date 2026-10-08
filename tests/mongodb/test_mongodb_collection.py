from unittest.mock import patch

import bson
import dlt
import pytest
from pymongo import MongoClient

from sources.mongodb import mongodb_collection

from tests.utils import assert_load_info, load_table_counts, select_data


@pytest.mark.parametrize("document_count", [0, 2], ids=["empty", "documents"])
def test_mongodb_collection_yields_documents(document_count: int) -> None:
    documents = [
        {"_id": bson.ObjectId(), "name": f"record_{i}"} for i in range(document_count)
    ]
    # decode synthetic query results with PyMongo's BSON codec
    query_results = [bson.decode(bson.encode(document)) for document in documents]

    with MongoClient("mongodb://localhost:1", connect=False) as client:
        collection = client["example"]["records"]
        with patch(
            "sources.mongodb.client_from_credentials", return_value=client
        ), patch.object(type(collection), "find", return_value=iter(query_results)):
            resource = mongodb_collection(
                connection_url="mongodb://localhost:1",
                database="example",
                collection="records",
                chunk_size=1,
                projection=None,
            )

            assert resource.name == "records"
            assert list(resource) == [
                {"_id": str(document["_id"]), "name": document["name"]}
                for document in documents
            ]


def test_mongodb_collection_loads_documents() -> None:
    documents = [
        {"_id": bson.ObjectId(), "name": "first"},
        {"_id": bson.ObjectId(), "name": "second"},
    ]
    query_results = [bson.decode(bson.encode(document)) for document in documents]
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_collection_return_type",
        destination="duckdb",
        dataset_name="mongodb_collection_data",
        dev_mode=True,
    )

    with MongoClient("mongodb://localhost:1", connect=False) as client:
        collection = client["example"]["records"]
        with patch(
            "sources.mongodb.client_from_credentials", return_value=client
        ), patch.object(type(collection), "find", return_value=iter(query_results)):
            resource = mongodb_collection(
                connection_url="mongodb://localhost:1",
                database="example",
                collection="records",
                chunk_size=1,
                write_disposition="replace",
                projection=None,
            )
            info = pipeline.run(resource)

    assert_load_info(info)
    assert load_table_counts(pipeline, "records") == {"records": 2}
    assert select_data(pipeline, 'SELECT "_id", name FROM records ORDER BY name') == [
        (str(document["_id"]), document["name"]) for document in documents
    ]
