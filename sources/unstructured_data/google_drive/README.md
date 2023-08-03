# Google Drive Source

This resource retrieves files from a specified Google Drive folder.

## Set credentials

[Read Quick Start with Google Drive:](https://developers.google.com/drive/api/quickstart/python?hl=en)

1. Enable Google Drive API.
1. Configure the OAuth consent screen.
1. Create OAuth client.
1. Create test user (if your app is not public)

After configuring "client_id", "client_secret" and "project_id" in "secrets.toml".
To generate the refresh token, run the following script from the root folder:

```shell
python3 google_drive/setup_script_gcp_oauth.py
```

Once you have executed the script and completed the authentication, you will receive a "refresh
token" that can be used to set up the "secrets.toml" configuration.

Set in `google_drive/settings.py` the storage folder path, it is the local folder where the
downloaded files will be stored:

```python
STORAGE_FOLDER_PATH = "google_drive/attachments"
```

List all Google Drive folders you want to extract files from:

```python
FOLDER_IDS = ["1-yiloGjyl9g40VguIE1QnY5tcRPaF0Nm"]
```

## Example

In the example below, the pipeline collects and downloads all files,
but only saves information about files with the "pdf" content_type to the `duckdb` database.

```python
import dlt

from unstructured_data.google_drive import google_drive_source

# configure the pipeline with your destination details
pipeline = dlt.pipeline(
    pipeline_name="unstructured_google_drive",
    destination="duckdb",
    dataset_name="unstructured_data_google_drive",
    full_refresh=True,
)

# use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
data_source = google_drive_source(download=True)

data_resource = data_source.resources["attachments"]
filtered_data_resource = data_resource.add_filter(
    lambda item: item["content_type"] == "application/pdf"
)

# run the pipeline with your parameters
load_info = pipeline.run(filtered_data_resource)
# pretty print the information on data that was loaded
print(load_info)
```
