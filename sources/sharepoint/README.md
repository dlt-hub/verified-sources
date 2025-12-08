# SharePoint Source

This source allows you to extract data from SharePoint lists and files using the Microsoft Graph API.

## Features

- Extract data from SharePoint lists
- Download and process files from SharePoint document libraries
- Support for multiple file formats (CSV, Excel, JSON, Parquet, SAS, SPSS)
- Incremental loading support for files based on modification time
- Flexible file filtering with regex patterns

## Prerequisites

Before using this source, you need:

1. **Azure AD Application Registration** with the following:
   - Client ID
   - Tenant ID
   - Client Secret
   - Microsoft Graph API permissions:
     - `Sites.Read.All` or `Sites.ReadWrite.All`
     - `Files.Read.All` (for file operations)

2. **SharePoint Site ID**: The unique identifier for your SharePoint site

## Configuration

### Credentials

Configure your credentials in `secrets.toml`:

```toml
[sources.sharepoint]
client_id = "your-client-id"
tenant_id = "your-tenant-id"
site_id = "your-site-id"
client_secret = "your-client-secret"
sub_site_id = ""  # Optional: for sub-sites
```

### SharePoint List Configuration

```python
from sharepoint.sharepoint_files_config import SharepointListConfig

list_config = SharepointListConfig(
    table_name="my_list_data",
    list_title="My SharePoint List",
    select="Title,Description,Status",  # Optional: specific fields
    is_incremental=False  # Incremental not yet implemented
)
```

### SharePoint Files Configuration

```python
from sharepoint.sharepoint_files_config import SharepointFilesConfig, FileType

files_config = SharepointFilesConfig(
    file_type=FileType.CSV,
    folder_path="Documents/Reports",
    table_name="reports_data",
    file_name_startswith="report_",
    pattern=r".*\.csv$",  # Optional: regex pattern for filtering
    pandas_kwargs={"sep": ","},  # Optional: pandas read options
    is_file_incremental=True  # Enable incremental loading
)
```

## Usage Examples

### Example 1: Load SharePoint List Data

```python
import dlt
from sharepoint import sharepoint_list, SharepointCredentials
from sharepoint.sharepoint_files_config import SharepointListConfig

# Configure credentials
credentials = SharepointCredentials()

# Configure list extraction
list_config = SharepointListConfig(
    table_name="tasks",
    list_title="Project Tasks"
)

# Create and run pipeline
pipeline = dlt.pipeline(
    pipeline_name="sharepoint_list",
    destination="duckdb",
    dataset_name="sharepoint_data"
)

load_info = pipeline.run(
    sharepoint_list(
        sharepoint_list_config=list_config,
        credentials=credentials
    )
)
print(load_info)
```

### Example 2: Load Files from SharePoint

```python
import dlt
from sharepoint import sharepoint_files, SharepointCredentials
from sharepoint.sharepoint_files_config import SharepointFilesConfig, FileType

# Configure credentials
credentials = SharepointCredentials()

# Configure file extraction
files_config = SharepointFilesConfig(
    file_type=FileType.CSV,
    folder_path="Shared Documents/Reports",
    table_name="monthly_reports",
    file_name_startswith="report_",
    pattern=r"202[4-5].*\.csv$",
    is_file_incremental=True,
    pandas_kwargs={"sep": ",", "encoding": "utf-8"}
)

# Create and run pipeline
pipeline = dlt.pipeline(
    pipeline_name="sharepoint_files",
    destination="duckdb",
    dataset_name="sharepoint_data"
)

load_info = pipeline.run(
    sharepoint_files(
        sharepoint_files_config=files_config,
        credentials=credentials
    )
)
print(load_info)
```

### Example 3: Process Excel Files with Chunking

```python
files_config = SharepointFilesConfig(
    file_type=FileType.EXCEL,
    folder_path="Reports/Annual",
    table_name="large_report",
    file_name_startswith="annual_",
    pandas_kwargs={
        "sheet_name": "Data",
        "chunksize": 1000  # Process in chunks of 1000 rows
    }
)
```

## Supported File Types

The source supports the following file types via pandas:

- `FileType.CSV` - CSV files
- `FileType.EXCEL` - Excel files (.xlsx, .xls)
- `FileType.JSON` - JSON files
- `FileType.PARQUET` - Parquet files
- `FileType.SAS` - SAS files
- `FileType.SPSS` - SPSS files

## Incremental Loading

### File Incremental Loading

When `is_file_incremental=True`, the source tracks the `lastModifiedDateTime` of files and only processes files that have been modified since the last run.

```python
files_config = SharepointFilesConfig(
    file_type=FileType.CSV,
    folder_path="Documents",
    table_name="data",
    file_name_startswith="data_",
    is_file_incremental=True  # Only process new/modified files
)
```

### List Incremental Loading

Incremental loading for SharePoint lists is not yet implemented.

## Advanced Configuration

### Folder Path Validation

Folder paths are automatically normalized:
- Leading/trailing slashes are removed
- Double slashes are not allowed
- Only alphanumeric characters, dashes, underscores, spaces, and dots are allowed

### Pattern Matching

The `pattern` parameter is automatically prefixed with `file_name_startswith`. For example:

```python
files_config = SharepointFilesConfig(
    file_name_startswith="report_",
    pattern=r"\d{8}\.csv$"
)
# Effective pattern: ^report_\d{8}\.csv$
```

### Pandas Kwargs

Any pandas read function parameters can be passed via `pandas_kwargs`:

```python
files_config = SharepointFilesConfig(
    file_type=FileType.CSV,
    folder_path="Documents",
    table_name="data",
    file_name_startswith="data",
    pandas_kwargs={
        "sep": ";",
        "encoding": "latin1",
        "decimal": ",",
        "chunksize": 5000
    }
)
```

## Troubleshooting

### Authentication Issues

If you encounter authentication errors:
1. Verify your Client ID, Tenant ID, and Client Secret are correct
2. Ensure your Azure AD app has the required permissions
3. Check that admin consent has been granted for the permissions

### File Not Found

If files are not being found:
1. Verify the folder path is correct (case-sensitive)
2. Check that the file name pattern matches your files
3. Ensure your app has access to the SharePoint site and folder

### Permission Errors

Ensure your Azure AD application has been granted:
- `Sites.Read.All` or `Sites.ReadWrite.All`
- `Files.Read.All`

And that admin consent has been provided for these permissions.

## Resources

- [Microsoft Graph API Documentation](https://learn.microsoft.com/en-us/graph/api/overview)
- [SharePoint REST API](https://learn.microsoft.com/en-us/sharepoint/dev/sp-add-ins/get-to-know-the-sharepoint-rest-service)
- [Azure AD App Registration](https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)
