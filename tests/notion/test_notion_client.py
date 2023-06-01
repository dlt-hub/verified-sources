from unittest.mock import patch, Mock
from pipelines.notion.helpers.client import NotionClient


@patch("dlt.sources.helpers.requests.get")
def test_fetch_resource(mock_get):
    mock_get.return_value.json.return_value = {"data": "test"}
    client = NotionClient(api_key="test_api_key")
    resource = client.fetch_resource("resource", "123")

    assert resource == {"data": "test"}
    mock_get.assert_called_once_with(
        "https://api.notion.com/v1/resource/123",
        headers=client._create_headers(),
    )


@patch("dlt.sources.helpers.requests.post")
def test_send_payload(mock_post):
    mock_post.return_value.json.return_value = {"data": "test"}
    client = NotionClient(api_key="test_api_key")
    response = client.send_payload("resource", "123", payload={"data": "payload"})

    assert response == {"data": "test"}
    mock_post.assert_called_once_with(
        "https://api.notion.com/v1/resource/123",
        headers=client._create_headers(),
        params=None,
        json={"data": "payload"},
    )


@patch.object(NotionClient, "send_payload")
def test_search(mock_send_payload):
    mock_send_payload.return_value = {
        "results": [{"id": "123", "title": "Test"}],
        "next_cursor": None,
    }
    client = NotionClient(api_key="test_api_key")
    results = list(
        client.search(filter_criteria={"value": "database", "property": "object"})
    )

    assert len(results) == 1
    assert results[0] == {"id": "123", "title": "Test"}
