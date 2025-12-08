import pytest
from unittest.mock import Mock, patch, MagicMock
from io import BytesIO
import pandas as pd
from typing import Dict, List

import dlt
from sources.sharepoint import (
    sharepoint_list,
    sharepoint_files,
    SharepointCredentials,
)
from sources.sharepoint.sharepoint_files_config import (
    SharepointFilesConfig,
    SharepointListConfig,
    FileType,
    validate_folder_path,
)
from sources.sharepoint.helpers import SharepointClient

from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
)


# Mock credentials for testing
MOCK_CREDENTIALS = {
    "client_id": "test_client_id",
    "tenant_id": "test_tenant_id",
    "site_id": "test_site_id",
    "client_secret": "test_client_secret",
    "sub_site_id": "",
}


class TestSharepointFilesConfig:
    """Test SharepointFilesConfig class"""

    def test_valid_config(self):
        """Test creating a valid SharepointFilesConfig"""
        config = SharepointFilesConfig(
            file_type=FileType.CSV,
            folder_path="/Documents/Reports",
            table_name="test_table",
            file_name_startswith="report",
            pattern=r".*\.csv$",
            pandas_kwargs={"sep": ","},
            is_file_incremental=True,
        )
        assert config.file_type == FileType.CSV
        assert config.folder_path == "Documents/Reports"
        assert config.table_name == "test_table"
        assert config.file_name_startswith == "report"
        assert config.pattern == r"^report.*\.csv$"
        assert config.pandas_kwargs == {"sep": ","}
        assert config.is_file_incremental is True

    def test_folder_path_normalization(self):
        """Test that folder paths are normalized correctly"""
        config = SharepointFilesConfig(
            file_type=FileType.EXCEL,
            folder_path="/Documents/",
            table_name="test_table",
            file_name_startswith="file",
        )
        assert config.folder_path == "Documents"

    def test_pattern_prefix(self):
        """Test that pattern is prefixed with file_name_startswith"""
        config = SharepointFilesConfig(
            file_type=FileType.CSV,
            folder_path="Documents",
            table_name="test_table",
            file_name_startswith="report_",
            pattern=r"\d{8}\.csv$",
        )
        assert config.pattern == r"^report_\d{8}\.csv$"

    def test_get_pd_function(self):
        """Test that get_pd_function returns correct pandas functions"""
        assert FileType.CSV.get_pd_function() == pd.read_csv
        assert FileType.EXCEL.get_pd_function() == pd.read_excel
        assert FileType.JSON.get_pd_function() == pd.read_json
        assert FileType.PARQUET.get_pd_function() == pd.read_parquet


class TestSharepointListConfig:
    """Test SharepointListConfig class"""

    def test_valid_config(self):
        """Test creating a valid SharepointListConfig"""
        config = SharepointListConfig(
            table_name="test_table",
            list_title="Test List",
            select="field1,field2",
            is_incremental=False,
        )
        assert config.table_name == "test_table"
        assert config.list_title == "Test List"
        assert config.select == "field1,field2"
        assert config.is_incremental is False

    def test_incremental_not_implemented(self):
        """Test that incremental loading raises NotImplementedError"""
        with pytest.raises(NotImplementedError):
            SharepointListConfig(
                table_name="test_table",
                list_title="Test List",
                is_incremental=True,
            )


class TestValidateFolderPath:
    """Test validate_folder_path function"""

    def test_remove_leading_slash(self):
        """Test that leading slashes are removed"""
        assert validate_folder_path("/Documents") == "Documents"

    def test_remove_trailing_slash(self):
        """Test that trailing slashes are removed"""
        assert validate_folder_path("Documents/") == "Documents"

    def test_remove_both_slashes(self):
        """Test that both leading and trailing slashes are removed"""
        assert validate_folder_path("/Documents/") == "Documents"

    def test_valid_path_with_subdirs(self):
        """Test valid path with subdirectories"""
        assert validate_folder_path("Documents/Reports/2024") == "Documents/Reports/2024"

    def test_valid_path_with_spaces(self):
        """Test valid path with spaces"""
        assert validate_folder_path("My Documents/My Reports") == "My Documents/My Reports"

    def test_invalid_characters(self):
        """Test that invalid characters raise ValueError"""
        with pytest.raises(ValueError, match="Invalid folder path"):
            validate_folder_path("Documents/Reports@2024")

    def test_double_slashes(self):
        """Test that double slashes raise ValueError"""
        with pytest.raises(ValueError, match="Invalid folder path with double slashes"):
            validate_folder_path("Documents//Reports")


class TestSharepointClient:
    """Test SharepointClient class"""

    def test_client_initialization(self):
        """Test SharepointClient initialization"""
        client = SharepointClient(**MOCK_CREDENTIALS)
        assert client.client_id == MOCK_CREDENTIALS["client_id"]
        assert client.tenant_id == MOCK_CREDENTIALS["tenant_id"]
        assert client.site_id == MOCK_CREDENTIALS["site_id"]
        assert client.client_secret == MOCK_CREDENTIALS["client_secret"]
        assert client.sub_site_id == ""
        assert client.graph_site_url == f"https://graph.microsoft.com/v1.0/sites/{MOCK_CREDENTIALS['site_id']}"

    def test_client_with_subsite(self):
        """Test SharepointClient initialization with sub_site_id"""
        credentials = MOCK_CREDENTIALS.copy()
        credentials["sub_site_id"] = "sub_site_123"
        client = SharepointClient(**credentials)
        expected_url = f"https://graph.microsoft.com/v1.0/sites/{MOCK_CREDENTIALS['site_id']}/sites/sub_site_123"
        assert client.graph_site_url == expected_url

    def test_client_missing_credentials(self):
        """Test that missing credentials raise ValueError"""
        with pytest.raises(ValueError, match="client_id, tenant_id, client_secret and site_id are required"):
            SharepointClient(
                client_id="",
                tenant_id="test",
                site_id="test",
                client_secret="test",
            )

    @patch("sources.sharepoint.helpers.ConfidentialClientApplication")
    @patch("sources.sharepoint.helpers.RESTClient")
    def test_connect_success(self, mock_rest_client, mock_msal_app):
        """Test successful connection to SharePoint"""
        # Mock MSAL token response
        mock_app_instance = Mock()
        mock_app_instance.acquire_token_for_client.return_value = {
            "access_token": "test_access_token"
        }
        mock_msal_app.return_value = mock_app_instance

        # Mock REST client
        mock_client_instance = Mock()
        mock_rest_client.return_value = mock_client_instance

        client = SharepointClient(**MOCK_CREDENTIALS)
        client.connect()

        assert client.client is not None
        mock_msal_app.assert_called_once()
        mock_rest_client.assert_called_once()

    @patch("sources.sharepoint.helpers.ConfidentialClientApplication")
    def test_connect_failure(self, mock_msal_app):
        """Test failed connection to SharePoint"""
        # Mock MSAL token response without access_token
        mock_app_instance = Mock()
        mock_app_instance.acquire_token_for_client.return_value = {"error": "authentication_failed"}
        mock_msal_app.return_value = mock_app_instance

        client = SharepointClient(**MOCK_CREDENTIALS)
        with pytest.raises(ConnectionError, match="Connection failed"):
            client.connect()

    @patch("sources.sharepoint.helpers.ConfidentialClientApplication")
    @patch("sources.sharepoint.helpers.RESTClient")
    def test_get_all_lists_in_site(self, mock_rest_client, mock_msal_app):
        """Test getting all lists from a site"""
        # Setup mocks
        mock_app_instance = Mock()
        mock_app_instance.acquire_token_for_client.return_value = {"access_token": "test_token"}
        mock_msal_app.return_value = mock_app_instance

        mock_client_instance = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {
            "value": [
                {
                    "id": "list1",
                    "displayName": "Test List 1",
                    "webUrl": "https://test.sharepoint.com/sites/test/Lists/TestList1",
                    "list": {"template": "genericList"},
                },
                {
                    "id": "list2",
                    "displayName": "Test List 2",
                    "webUrl": "https://test.sharepoint.com/sites/test/Lists/TestList2",
                    "list": {"template": "genericList"},
                },
                {
                    "id": "list3",
                    "displayName": "Document Library",
                    "webUrl": "https://test.sharepoint.com/sites/test/Shared Documents",
                    "list": {"template": "documentLibrary"},
                },
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_rest_client.return_value = mock_client_instance

        client = SharepointClient(**MOCK_CREDENTIALS)
        client.connect()
        lists = client.get_all_lists_in_site()

        assert len(lists) == 2
        assert all(item["list"]["template"] == "genericList" for item in lists)
        assert all("Lists" in item["webUrl"] for item in lists)

    @patch("sources.sharepoint.helpers.ConfidentialClientApplication")
    @patch("sources.sharepoint.helpers.RESTClient")
    def test_get_items_from_list(self, mock_rest_client, mock_msal_app):
        """Test getting items from a specific list"""
        # Setup mocks
        mock_app_instance = Mock()
        mock_app_instance.acquire_token_for_client.return_value = {"access_token": "test_token"}
        mock_msal_app.return_value = mock_app_instance

        mock_client_instance = Mock()

        # Mock response for get_all_lists_in_site
        mock_lists_response = Mock()
        mock_lists_response.json.return_value = {
            "value": [
                {
                    "id": "list1",
                    "displayName": "Test List",
                    "webUrl": "https://test.sharepoint.com/sites/test/Lists/TestList",
                    "list": {"template": "genericList"},
                }
            ]
        }
        mock_lists_response.raise_for_status = Mock()

        # Mock response for list items
        mock_items_response = Mock()
        mock_items_response.json.return_value = {
            "value": [
                {"fields": {"Title": "Item 1", "Description": "Test item 1"}},
                {"fields": {"Title": "Item 2", "Description": "Test item 2"}},
            ]
        }
        mock_items_response.raise_for_status = Mock()

        mock_client_instance.get.side_effect = [mock_lists_response, mock_items_response]
        mock_rest_client.return_value = mock_client_instance

        client = SharepointClient(**MOCK_CREDENTIALS)
        client.connect()
        items = client.get_items_from_list("Test List")

        assert len(items) == 2
        assert items[0]["Title"] == "Item 1"
        assert items[1]["Title"] == "Item 2"

    @patch("sources.sharepoint.helpers.ConfidentialClientApplication")
    @patch("sources.sharepoint.helpers.RESTClient")
    def test_get_items_from_nonexistent_list(self, mock_rest_client, mock_msal_app):
        """Test getting items from a list that doesn't exist"""
        # Setup mocks
        mock_app_instance = Mock()
        mock_app_instance.acquire_token_for_client.return_value = {"access_token": "test_token"}
        mock_msal_app.return_value = mock_app_instance

        mock_client_instance = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {
            "value": [
                {
                    "id": "list1",
                    "displayName": "Test List",
                    "webUrl": "https://test.sharepoint.com/sites/test/Lists/TestList",
                    "list": {"template": "genericList"},
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_rest_client.return_value = mock_client_instance

        client = SharepointClient(**MOCK_CREDENTIALS)
        client.connect()

        with pytest.raises(ValueError, match="List with title 'Nonexistent List' not found"):
            client.get_items_from_list("Nonexistent List")

    @patch("sources.sharepoint.helpers.ConfidentialClientApplication")
    @patch("sources.sharepoint.helpers.RESTClient")
    def test_get_files_from_path(self, mock_rest_client, mock_msal_app):
        """Test getting files from a folder path"""
        # Setup mocks
        mock_app_instance = Mock()
        mock_app_instance.acquire_token_for_client.return_value = {"access_token": "test_token"}
        mock_msal_app.return_value = mock_app_instance

        mock_client_instance = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {
            "value": [
                {
                    "name": "report_2024.csv",
                    "file": {},
                    "lastModifiedDateTime": "2024-01-01T00:00:00Z",
                },
                {
                    "name": "report_2023.csv",
                    "file": {},
                    "lastModifiedDateTime": "2023-01-01T00:00:00Z",
                },
                {
                    "name": "subfolder",
                    "folder": {},
                },
            ]
        }
        mock_client_instance.get.return_value = mock_response
        mock_rest_client.return_value = mock_client_instance

        client = SharepointClient(**MOCK_CREDENTIALS)
        client.connect()
        files = client.get_files_from_path("Documents", "report", pattern=r".*2024.*")

        assert len(files) == 1
        assert files[0]["name"] == "report_2024.csv"

    @patch("sources.sharepoint.helpers.ConfidentialClientApplication")
    @patch("sources.sharepoint.helpers.RESTClient")
    def test_get_file_bytes_io(self, mock_rest_client, mock_msal_app):
        """Test downloading a file to BytesIO"""
        # Setup mocks
        mock_app_instance = Mock()
        mock_app_instance.acquire_token_for_client.return_value = {"access_token": "test_token"}
        mock_msal_app.return_value = mock_app_instance

        mock_client_instance = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"test file content"
        mock_client_instance.get.return_value = mock_response
        mock_rest_client.return_value = mock_client_instance

        client = SharepointClient(**MOCK_CREDENTIALS)
        client.connect()

        file_item = {
            "name": "test.csv",
            "@microsoft.graph.downloadUrl": "https://test.sharepoint.com/download/test.csv",
        }
        bytes_io = client.get_file_bytes_io(file_item)

        assert isinstance(bytes_io, BytesIO)
        assert bytes_io.getvalue() == b"test file content"


class TestSharepointListSource:
    """Test sharepoint_list source"""

    @patch("sources.sharepoint.SharepointClient")
    def test_sharepoint_list_source(self, mock_client_class):
        """Test sharepoint_list source yields data correctly"""
        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.site_info = {"id": "test_site"}
        mock_client_instance.get_items_from_list.return_value = [
            {"Title": "Item 1", "Field1": "Value1"},
            {"Title": "Item 2", "Field1": "Value2"},
        ]
        mock_client_class.return_value = mock_client_instance

        # Create config
        config = SharepointListConfig(
            table_name="test_table",
            list_title="Test List",
            select="Title,Field1",
        )

        # Create source
        credentials = SharepointCredentials(**MOCK_CREDENTIALS)
        source = sharepoint_list(config, credentials=credentials)

        # Extract resources from source
        resources = list(source.resources.values())
        assert len(resources) == 1

        # Get data from resource
        resource_data = list(resources[0])
        assert len(resource_data) == 2
        assert resource_data[0]["Title"] == "Item 1"
        assert resource_data[1]["Title"] == "Item 2"
class TestSharepointFilesSource:
    """Test sharepoint_files source"""

    @patch("sources.sharepoint.SharepointClient")
    def test_sharepoint_files_source_csv(self, mock_client_class):
        """Test sharepoint_files source with CSV files"""
        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.site_info = {"id": "test_site"}
        mock_client_instance.get_files_from_path.return_value = [
            {
                "name": "report.csv",
                "lastModifiedDateTime": "2024-01-01T00:00:00Z",
                "@microsoft.graph.downloadUrl": "https://test.com/report.csv",
            }
        ]

        # Create test CSV data
        csv_data = b"col1,col2\nval1,val2\nval3,val4"
        mock_client_instance.get_file_bytes_io.return_value = BytesIO(csv_data)
        mock_client_class.return_value = mock_client_instance

        # Create config
        config = SharepointFilesConfig(
            file_type=FileType.CSV,
            folder_path="Documents",
            table_name="test_table",
            file_name_startswith="report",
        )

        # Create source
        credentials = SharepointCredentials(**MOCK_CREDENTIALS)
        source = sharepoint_files(config, credentials=credentials)

        # Extract resources from source
        resources = list(source.resources.values())
        assert len(resources) == 1

        # Get data from resource - this should yield dataframes
        all_data = list(resources[0])

        # The transformer should yield dataframes
        assert len(all_data) > 0

    @patch("sources.sharepoint.SharepointClient")
    def test_sharepoint_files_source_incremental(self, mock_client_class):
        """Test sharepoint_files source with incremental loading"""
        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.site_info = {"id": "test_site"}
        mock_client_instance.get_files_from_path.return_value = [
            {
                "name": "old_file.csv",
                "lastModifiedDateTime": "2020-01-01T00:00:00Z",
                "@microsoft.graph.downloadUrl": "https://test.com/old_file.csv",
            },
            {
                "name": "new_file.csv",
                "lastModifiedDateTime": "2024-01-01T00:00:00Z",
                "@microsoft.graph.downloadUrl": "https://test.com/new_file.csv",
            },
        ]

        csv_data = b"col1,col2\nval1,val2"
        mock_client_instance.get_file_bytes_io.return_value = BytesIO(csv_data)
        mock_client_class.return_value = mock_client_instance

        # Create config with incremental loading
        config = SharepointFilesConfig(
            file_type=FileType.CSV,
            folder_path="Documents",
            table_name="test_table",
            file_name_startswith="file",
            is_file_incremental=True,
        )

        # Create source
        credentials = SharepointCredentials(**MOCK_CREDENTIALS)
        source = sharepoint_files(config, credentials=credentials)

        # Extract resources from source
        resources = list(source.resources.values())
        assert len(resources) == 1

    @patch("sources.sharepoint.SharepointClient")
    def test_sharepoint_files_source_with_chunks(self, mock_client_class):
        """Test sharepoint_files source with chunked reading"""
        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.site_info = {"id": "test_site"}
        mock_client_instance.get_files_from_path.return_value = [
            {
                "name": "large_file.csv",
                "lastModifiedDateTime": "2024-01-01T00:00:00Z",
                "@microsoft.graph.downloadUrl": "https://test.com/large_file.csv",
            }
        ]

        # Create larger CSV data
        csv_data = b"col1,col2\n" + b"\n".join([b"val1,val2" for _ in range(100)])
        mock_client_instance.get_file_bytes_io.return_value = BytesIO(csv_data)
        mock_client_class.return_value = mock_client_instance

        # Create config with chunksize
        config = SharepointFilesConfig(
            file_type=FileType.CSV,
            folder_path="Documents",
            table_name="test_table",
            file_name_startswith="large",
            pandas_kwargs={"chunksize": 10},
        )

        # Create source
        credentials = SharepointCredentials(**MOCK_CREDENTIALS)
        source = sharepoint_files(config, credentials=credentials)

        # Extract resources from source
        resources = list(source.resources.values())
        assert len(resources) == 1

        # Get data from resource - with chunksize, this yields multiple dataframes (chunks)
        all_chunks = list(resources[0])
        # Should have 10 chunks (100 rows / 10 rows per chunk)
        assert len(all_chunks) == 10


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_sharepoint_list_pipeline(destination_name: str) -> None:
    """Integration test for sharepoint_list pipeline"""

    with patch("sources.sharepoint.SharepointClient") as mock_client_class:
        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.site_info = {"id": "test_site", "displayName": "Test Site"}
        mock_client_instance.get_items_from_list.return_value = [
            {"Title": "Item 1", "Description": "Description 1", "Status": "Active"},
            {"Title": "Item 2", "Description": "Description 2", "Status": "Completed"},
            {"Title": "Item 3", "Description": "Description 3", "Status": "Active"},
        ]
        mock_client_class.return_value = mock_client_instance

        # Create pipeline
        pipeline = dlt.pipeline(
            pipeline_name="test_sharepoint_list",
            destination=destination_name,
            dataset_name="sharepoint_list_test",
            dev_mode=True,
        )

        # Create config
        config = SharepointListConfig(
            table_name="test_items",
            list_title="Test List",
        )

        # Create source and run pipeline
        credentials = SharepointCredentials(**MOCK_CREDENTIALS)
        source = sharepoint_list(config, credentials=credentials)
        load_info = pipeline.run(source)

        # Assert load info
        assert_load_info(load_info)

        # Check table counts
        table_counts = load_table_counts(pipeline, "test_items")
        assert table_counts["test_items"] == 3


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_sharepoint_files_pipeline(destination_name: str) -> None:
    """Integration test for sharepoint_files pipeline"""

    with patch("sources.sharepoint.SharepointClient") as mock_client_class:
        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.site_info = {"id": "test_site", "displayName": "Test Site"}
        mock_client_instance.get_files_from_path.return_value = [
            {
                "name": "data.csv",
                "lastModifiedDateTime": "2024-01-01T00:00:00Z",
                "@microsoft.graph.downloadUrl": "https://test.com/data.csv",
            }
        ]

        # Create test CSV data
        csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago"
        mock_client_instance.get_file_bytes_io.return_value = BytesIO(csv_data)
        mock_client_class.return_value = mock_client_instance

        # Create pipeline
        pipeline = dlt.pipeline(
            pipeline_name="test_sharepoint_files",
            destination=destination_name,
            dataset_name="sharepoint_files_test",
            dev_mode=True,
        )

        # Create config
        config = SharepointFilesConfig(
            file_type=FileType.CSV,
            folder_path="Documents",
            table_name="test_data",
            file_name_startswith="data",
        )

        # Create source and run pipeline
        credentials = SharepointCredentials(**MOCK_CREDENTIALS)
        source = sharepoint_files(config, credentials=credentials)
        load_info = pipeline.run(source)

        # Assert load info
        assert_load_info(load_info)

        # Check table counts
        table_counts = load_table_counts(pipeline, "test_data")
        assert table_counts["test_data"] == 3
