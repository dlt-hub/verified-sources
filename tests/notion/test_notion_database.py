import pytest
from unittest.mock import patch, Mock
from pipelines.notion.database import NotionDatabase
from pipelines.notion.client import NotionClient


@patch.object(NotionClient, "fetch_resource")
def test_get_structure(mock_fetch_resource):
    mock_fetch_resource.return_value = {
        "object": "database",
        "id": "bc1211ca-e3f1-4939-ae34-5260b16f627c",
    }
    client = NotionClient(api_key="test_api_key")
    database = NotionDatabase("database_id", client)

    structure = database.get_structure()

    assert structure == {
        "object": "database",
        "id": "bc1211ca-e3f1-4939-ae34-5260b16f627c",
    }
    mock_fetch_resource.assert_called_once_with("databases", "database_id")


@patch.object(NotionClient, "send_payload")
def test_query(mock_send_payload):
    mock_send_payload.return_value = {
        "results": [{"id": "123", "title": "Test Record"}],
        "has_more": False,
    }
    client = NotionClient(api_key="test_api_key")
    database = NotionDatabase("database_id", client)

    results = list(
        database.query(filter_criteria={"value": "database", "property": "object"})
    )

    assert len(results) == 1
    assert results[0] == [{"id": "123", "title": "Test Record"}]
