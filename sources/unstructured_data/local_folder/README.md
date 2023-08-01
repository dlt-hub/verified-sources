# Local folder Source
This resource retrieves a list of files from a local folder in format:
```
[{"file_path": "path/to/the/file", ...}, ...]
```
and store it in the destination.

### Configuration
Set the path to the local data folder in `local_folder_source` resource function:

```python
local_folder_source(data_dir="path/to/the/folder")
```

### Example
```python
from local_folder import local_folder_source

# configure the pipeline with your destination details
pipeline = dlt.pipeline(
    pipeline_name="local_folder",
    destination="duckdb",
    dataset_name="data_local_folder",
    full_refresh=True,
)

# use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
data_resource = local_folder_source(data_dir="path/to/the/folder")
# run the pipeline with your parameters
load_info = pipeline.run(data_resource)
# pretty print the information on data that was loaded
print(load_info)
```