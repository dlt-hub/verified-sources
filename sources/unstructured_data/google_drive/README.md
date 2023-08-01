# Google Drive Source

This resource retrieves files from a specified Google Drive folder.
## Set credentials

[Read Quick Start with Google Drive:](https://developers.google.com/drive/api/quickstart/python?hl=en)

1. Enable Google Drive API.
2. Configure the OAuth consent screen.
3. Create credentials json.
4. Create test user (if your app is not public)

Save the path to this json in `google_drive/settings.py`:
```python
ClIENT_SECRET_PATH = "client_secret.json"
```

If you already have the **authorized** user json file "token.json", then put it in a `google_drive/settings.py` file:
```python
AUTHORIZED_USER_PATH = "/path/to/token.json"
```
or you can use the authorized user info from this json directly, copy info from json to `.dlt/secrets.toml`:
```toml
[sources.google_drive.credentials]
token = "<token>"
refresh_token = "<refresh_token>"
token_uri = "<token_uri>"
client_id = "<client_id>"
client_secret = "<client_secret>"
scopes = ["<scopes>"]
expiry = "<expiry>"
```

Set in `google_drive/settings.py` the storage folder path, it is the local folder where the downloaded files will be stored:
```python
STORAGE_FOLDER_PATH = "google_drive/attachments"
```
List all Google Drive folders you want to extract files from:
```python
FOLDER_IDS = ["1-yiloGjyl9g40VguIE1QnY5tcRPaF0Nm"]
```

## Example
```python
import dlt

from google_drive import google_drive_source

# configure the pipeline with your destination details
pipeline = dlt.pipeline(
    pipeline_name="google_drive",
    destination="duckdb",
    dataset_name="data_google_drive",
    full_refresh=True,
)

# use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
data_resource = google_drive_source(download=False, extensions=(".txt", ".pdf", ".jpg"))
# run the pipeline with your parameters
load_info = pipeline.run(data_resource)
# pretty print the information on data that was loaded
print(load_info)
```