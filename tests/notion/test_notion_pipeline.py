from unittest.mock import patch, Mock
from pipelines.notion import notion_databases
from pipelines.notion.client import NotionClient
from pipelines.notion.database import NotionDatabase


@patch.object(NotionDatabase, 'query')
@patch('pipelines.notion.NotionClient')
def test_notion_databases(mock_NotionClient, mock_query):
    mock_client_instance = mock_NotionClient.return_value
    mock_client_instance.search.return_value = [
        {'id': '123', 'title': [{'plain_text': 'Test Database'}]}
    ]
    mock_query.return_value = [{'id': '123', 'name': 'Test Record'}]

    databases = list(notion_databases(api_key='test_api_key'))

    mock_NotionClient.assert_called_once_with('test_api_key')
    mock_client_instance.search.assert_called_once_with(
        filter_criteria={'value': 'database', 'property': 'object'}
    )
    mock_query.assert_called_once()

    assert len(databases) == 1
    assert databases[0] == {'id': '123', 'name': 'Test Record'}


@patch.object(NotionDatabase, 'query')
@patch('pipelines.notion.NotionClient')
def test_notion_databases_with_database_ids(mock_NotionClient, mock_query):
    mock_client_instance = mock_NotionClient.return_value
    mock_query.return_value = [{'id': '123', 'name': 'Test Record'}]

    databases = list(
        notion_databases(
            database_ids=[{'id': '123', 'use_name': 'Test Database'}],
            api_key='test_api_key',
        )
    )

    mock_NotionClient.assert_called_once_with('test_api_key')
    mock_client_instance.search.assert_not_called()
    mock_query.assert_called_once()

    assert len(databases) == 1
    assert databases[0] == {'id': '123', 'name': 'Test Record'}


@patch.object(NotionDatabase, 'query')
@patch('pipelines.notion.NotionClient')
def test_notion_databases_no_databases(mock_NotionClient, mock_query):
    mock_client_instance = mock_NotionClient.return_value
    mock_client_instance.search.return_value = []

    databases = list(notion_databases(api_key='test_api_key'))

    assert len(databases) == 0
