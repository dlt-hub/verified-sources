# Readers Source & Filesystem

This verified source easily streams files from AWS S3, GCS, Azure, or local filesystem using the
reader source.

Sources and resources that can be used with this verified source are:


| Name         | Type                 | Description                                                               |
|--------------|----------------------|---------------------------------------------------------------------------|
| readers      | Source               | Lists and reads files with resource `filesystem` and readers transformers |
| filesystem   | Resource             | Lists files in `bucket_url` using `file_glob` pattern                     |
| read_csv     | Resource-transformer | Reads CSV file with "Pandas" chunk by chunk                               |
| read_jsonl   | Resource-transformer | Reads JSONL file content and extracts the data                            |
| read_parquet | Resource-transformer | Reads Parquet file content and extracts the data with "Pyarrow"           |

## Using standalone filesystem resources

Utilize `filesystem`, a
[standalone resource](https://dlthub.com/docs/general-usage/resource#declare-a-standalone-resource),
to enumerate S3, GCS, and Azure bucket files. Customize file readers or manage files as needed.
"fsspec" underpins our system; for details, see the
[fsspec documentation](https://filesystem-spec.readthedocs.io/en/latest/index.html). These building
blocks enable you to rapidly develop pipelines for:

- Extracting and parsing text from PDFs.
- Streaming content from large files in a bucket.
- Locally copying files

For examples, see [sources/filesystem_pipeline.py](../filesystem_pipeline.py) and resources in
[filesystem/readers.py](../filesystem/readers.py).

Assign a
[unique name](https://dlthub.com/docs/general-usage/resource#duplicate-and-rename-resources) to each
resource prior to executing `pipeline.run`. This ensures data is directed to your desired table
and maintains distinct states for incremental loads.

> To add a new file reader is straightforward. For demos, see
> ["filesystem_pipeline.py"](../filesystem_pipeline.py). We welcome contributions for any file types,
> including PDFs and Excel files.

## Initialize the source

```shell
dlt init filesystem duckdb
```

Here, we chose duckdb as the destination. Alternatively, you can also choose redshift, bigquery, or
any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source

To grab the credentials for AWS S3, Google Cloud Storage, Azure cloud storage and initialize the
pipeline, please refer to the
[full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem)

## Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   [sources.readers.credentials] # use [sources.readers.credentials] for the "readers" source
   # For AWS S3 access:
   aws_access_key_id="Please set me up!"
   aws_secret_access_key="Please set me up!"

   # For GCS storage bucket access:
   client_email="Please set me up!"
   private_key="Please set me up!"
   project_id="Please set me up!"

   # For Azure blob storage access:
   azure_storage_account_name="Please set me up!"
   azure_storage_account_key="Please set me up!"
   ```

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

1. You can pass the bucket URL and glob pattern or use `config.toml`. For local filesystems, use
   `file://` or skip the schema.

   ```toml
   [sources.filesystem] # use [sources.readers.credentials] for the "readers" source
   bucket_url="~/Documents/csv_files/"
   file_glob="*"
   ```

   For remote file systems you need to add the schema, it will be used to get the protocol being
   used, for example:

   ```toml
   [sources.filesystem] # use [sources.readers.credentials] for the "readers" source
   bucket_url="s3://my-bucket/csv_files/"
   ```

   > caution For Azure, use adlfs>=2023.9.0. Older versions mishandle globs.

## Run the pipeline

For running the pipeline and installing dependencies, please refer to the original
[documentation.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem#run-the-pipeline)

## Filesystem Integration and Data Extraction Guide

To read more about filesystem usage, fileitem representation and file manipulation refer to our
official
[documentation.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem#filesystem-integration-and-data-extraction-guide)

## Examples

This example demonstrates opening a large CSV file from supported buckets (or local filesystem),
streaming it in configurable chunks, and loading it directly from the bucket.

```python
@dlt.transformer(standalone=True)
def read_csv(
    items: Iterable[FileItemDict],
    chunksize: int = 15,
) -> Iterator[TDataItems]:
    """Reads csv file with Pandas chunk by chunk.

    Args:
        item (TDataItem): The list of files to copy.
        chunksize (int): Number of records to read in one chunk
    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        # Here we use pandas chunksize to read the file in chunks and avoid loading the whole file
        # in memory.
        with file_obj.open() as file:
            for df in pd.read_csv(
                file,
                header="infer",
                chunksize=chunksize,
            ):
                yield df.to_dict(orient="records")

pipeline = dlt.pipeline(
    pipeline_name="standard_filesystem_csv",
    destination="duckdb",
    dataset_name="csv_data",
)
# Load all the CSV files in the "directory" folder
BUCKET_URL = "YOUR_BUCKET_PATH_HERE"   # path of the bucket url or local destination
csv_files = (
    filesystem(bucket_url=BUCKET_URL, file_glob="directory/*.csv")
    | read_csv()
)
# NOTE: data is loaded to "csv_data" table
load_info = pipeline.run(csv_files.with_name("csv_data"))
print(load_info)
print(pipeline.last_trace.last_normalize_info)
```

> Similarly, you can create transformer functions for "jsonl" and "parquet" formats. For further
> details, refer to [readers.py.](../filesystem/readers.py)

### Incremental loading pipeline

Convert the filesystem resource into an incremental one using `primary_key` on "file_url" and
"modification_time" in each FileItem. The example below retrieves files modified or created since the
last run:

```python
pipeline = dlt.pipeline(
    pipeline_name="standard_filesystem_incremental",
    destination="duckdb",
    dataset_name="file_tracker",
)
BUCKET_URL = "YOUR_BUCKET_PATH_HERE"   # path of the bucket url or local destination

#Modify the filesystem resource to track only new CSV files, which can then be paired
# with a transformer for further processing

new_files = filesystem(bucket_url=BUCKET_URL, file_glob="csv/*")
# Enable incremental loading based on modification time.
new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
load_info = pipeline.run((new_files | read_csv()).with_name("csv_files"))
print(load_info)
print(pipeline.last_trace.last_normalize_info)

# load again - no new files!
new_files = filesystem(bucket_url=BUCKET_URL, file_glob="csv/*")
# add incremental on modification time
new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
load_info = pipeline.run((new_files | read_csv()).with_name("csv_files"))
print(load_info)
print(pipeline.last_trace.last_normalize_info)
```

Observe the use of apply_hints for incremental loading based on modification_date in new_files.
Running it twice illustrates that files from the first run are excluded.

### Cleanup after loading data

Obtain an "fsspec" client from an extracted filesystem resource to perform operations like deleting
processed files. Use the `fsspec_from_resource` method from the filesystem module as shown:

```python
from filesystem import filesystem, fsspec_from_resource
# get filesystem source
gs_resource = filesystem("gs://ci-test-bucket/")
# extract files
pipeline.run(gs_resource | read_csv)
# get fs client
fs_client = fsspec_from_resource(gs_resource)
# do any operation
fs_client.ls("ci-test-bucket/standard_source/samples")
```
