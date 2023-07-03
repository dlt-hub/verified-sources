# Filesystem

## Local folder
This resource retrieves a list of files from a local folder in format:
```
[{"file_path": "path/to/the/file"}, ...]
```
and store it in the destination:

### Set credentials
Set the path to the local data folder in `.dlt/secrets.toml`:
```toml
[sources.filesystem.local_folder]
data_dir = "/path/to/your/local/data/folder"
```

### Run pipeline
```python
import dlt

from filesystem import local_folder

# configure the pipeline with your destination details
pipeline = dlt.pipeline(
    pipeline_name="local_folder",
    destination="duckdb",
    dataset_name="data_from_local_folder",
    full_refresh=True,
)

# use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
data_resource = local_folder(extensions=(".txt", ".pdf"))

# run the pipeline with your parameters
load_info = pipeline.run(data_resource)
# pretty print the information on data that was loaded
print(load_info)

```


## Google Drive API
### Set credentials

[Read Quick Start with Google Drive:](https://developers.google.com/drive/api/quickstart/python?hl=en)

1. Enable Google Drive API.
2. Configure the OAuth consent screen.
3. Create credentials json.
4. Save the path to this json in `.dlt/secrets.toml`:
    ```toml
    [sources.filesystem.google_drive]
    credentials_path = '/path/to/your/credentials.json'
    folder_id = 'folder_id'
    storage_folder_path = './temp'
    ```

### Run pipeline
```python
import dlt

from filesystem import google_drive

# configure the pipeline with your destination details
pipeline = dlt.pipeline(
    pipeline_name="google_drive",
    destination="duckdb",
    dataset_name="data_from_google_drive",
    full_refresh=True,
)

# use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
data_resource = google_drive(extensions=(".txt", ".pdf"))

# run the pipeline with your parameters
load_info = pipeline.run(data_resource)
# pretty print the information on data that was loaded
print(load_info)
```
