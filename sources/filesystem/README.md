---
title: Filesystem
description: dlt source for fsspec filesystems and file readers
keywords: [filesystem, fsspec, s3, gcs, azure blob storage]
---

# Readers Source & Filesystem
Use the `readers` source to easily stream (in chunks) files from s3, gcs, azure buckets or local filesystem. Currently we support following readers
* **read_csv** (with Pandas)
* **read_jsonl**
* **read_parquet** (with pyarrow)

TODO: show example - link to specific demos
It is extremely easy to add a new file reader! If you have anything neat (PDFs? excel files) - please contribute



Use **filesystem** [standalone resource](https://dlthub.com/docs/general-usage/resource#declare-a-standalone-resource) to lists files in s3, gcs and azure buckets
and **create your customized file readers or do anything else with the files**. Internally we use **fsspec**. For more information about **fsspec** please visit [fsspec documentation](https://filesystem-spec.readthedocs.io/en/latest/index.html).
**filesystem** represents files uniformly for all bucket types and provides convenience methods to open them
and read the data. Those building blocks let you very quickly create pipelines that:
* read file content and parse text out of PDFs
* stream the content of large files files directly from the bucket
* copy the files locally

Please refer to examples in [sources/filesystem_pipeline.py](../../filesystem_pipeline.py)

**We recommend that you give each resource a [specific name](https://dlthub.com/docs/general-usage/resource#duplicate-and-rename-resources)** before loading with `pipeline.run`. This will make sure that data goes to a table with
the name you want and that each pipeline [uses a separate state for incremental loading](https://dlthub.com/docs/general-usage/state#read-and-write-pipeline-state-in-a-resource).

## Initialize the source

Initialize the source with dlt command:

```shell
dlt init filesystem duckdb
```

## Set filesystem credentials

**skip if you read local files**

1. Open `.dlt/secrets.toml`.
2. Please read the [corresponding documentation](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem) on **filesystem** destination which explains the setup of all bucket types.
   Just remember to replace `[destination.filesystem.*]` with `[sources.filesystem.*]` in all config/secret entries ie.
   ```toml
   [sources.filesystem.credentials]
   aws_access_key_id="..."
   aws_secret_access_key="..."
   ```
3. You can pass the **bucket url** and **glob pattern** in code. You can also do that via `config.toml`: Add the bucket url which contains the path to the files, if you are using a local filesystem,
   you can use the `file://` schema or just ignore the schema. For example:
   ```toml
   [sources.filesystem]
   bucket_url="~/Documents/csv_files/"
   file_glob="*" # the default
   ```
   For remote file systems you need to add the schema, it will be used to get the protocol being
   used, for example:
   ```toml
   [sources.filesystem]
   bucket_url="s3://my-bucket/csv_files/"
   ```

**Note:** for azure use **adlfs>=2023.9.0** Older version do not handle globs correctly.
## Filesystem Usage

The **filesystem** resource will list files in selected bucket using specified glob pattern and return information on each file (`FileInfo` below) in pages of configurable size.
The resource is designed to work with [transform functions](https://dlthub.com/docs/general-usage/resource#filter-transform-and-pivot-data)
and [transformers](https://dlthub.com/docs/general-usage/resource#process-resources-with-dlttransformer) that should be used to build specialized extract pipelines. Typically you also want to load data into a table with a given name (and not **filesystem** table which is the default). Snippet below illustrates both:
```python
@dlt.transformer(standalone=True)
def read_csv(items, chunksize: int = 15) ->:
    """Reads csv file with Pandas chunk by chunk."""
    ...

# list only the *.csv in specific folder and pass the file items to read_csv()
met_files = (
    filesystem(bucket_url="s3://my_bucket/data, file_glob="met_csv/A801/*.csv")
    | read_csv()
    )
# load to met_csv table using with_name()
pipeline.run(met_files.with_name("met_csv"))
```


`filesystem` resource takes following parameters:
* **bucket_url**: An url to a bucket
* **credentials**: One of the bucket credentials (typically from config) or `AbstractFilesystem` (fsspec client) instance
* **extract_content**: If set to `True`, the content of the file will be read and returned in the
  resource. Defaults to `False`.
* **file_glob**: A glob pattern supported by [fsspec](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.glob). Defaults to `*` (all files from current folder, non recursive)
* **chunksize**: The number of files that will be read at once. This is useful to avoid reading all
  the files at once, which can be a problem if you have a lot of files or if the files are big and
  you are using the `extract_content` parameter.


### The `FileItem` file representation
All `dlt` resources/sources that yield files adhere to `FileItem` contract. The content of the file is not accessed/loaded (if possible) - instead a full file information is available together with methods to open a file and read file content. Users can also request an authenticated **filespec** `AbstractFilesystem` instance. `FileItem` is a TypedDict with following fields
* **file_url** - full url to the file, also used as primary key. Always contains schema parti (ie. **file://**)
* **file_name** - a name of the file or path to the file in relation to the `bucket_url`
* **mime_type** - a mime type of the file provided on the best effort basis. Taken from bucket provider is available, otherwise we guess it from the file extension.
* **modification_date** - modification time of the file. Always `pendulum.DateTime`
* **size_in_bytes**
* **file_content** - a content of the file (if specifically requested)

Note that **file_name** will contain a path to file if nested/recursive glob is used. For example the following resource:
```python
filesystem("az://dlt-ci-test-bucket/standard_source/samples", file_glob="met_csv/A801/*.csv")
```
will yield file names relative to **/standard_source/samples** path ie. **met_csv/A801/A881_20230920.csv**.

### Open, read and manipulate files during extraction
The `FileItem` is backed by an `dict` implementation that provides following helper methods:
- **read_bytes()**: Reads the file and returns the content as **bytes**.
- **open()**: Opens the file and returns a file object.
And property
- **filesystem**: returns authorized `AbstractFilesystem` with all usual fsspec methods.

### Read csvs example
This example shows how a very large csv file can be opened on any of supported buckets (or local file system) and then streamed in
configurable chunks and loaded directly from the bucket.

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
    dataset_name="met_data",
)
# load all csvs in A801 folder
met_files = (
    filesystem(bucket_url=TESTS_BUCKET_URL, file_glob="met_csv/A801/*.csv")
    | read_csv()
)
# NOTE: we load to met_csv table
load_info = pipeline.run(met_files.with_name("met_csv"))
print(load_info)
print(pipeline.last_trace.last_normalize_info)
```

### Incremental loading
You can convert `filesystem` resource into incremental one. It already defines `primary_key` on `file_url` and each `FileItem` contains `modification_time`. Following example will return only files that were modified/created from the previous run:
```python
pipeline = dlt.pipeline(
    pipeline_name="standard_filesystem_incremental",
    destination="duckdb",
    dataset_name="file_tracker",
)

# here we modify filesystem resource so it will track only new csv files
# such resource may be then combined with transformer doing further processing
new_files = filesystem(bucket_url=TESTS_BUCKET_URL, file_glob="csv/*")
# add incremental on modification time
new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
load_info = pipeline.run((new_files | read_csv()).with_name("csv_files"))
print(load_info)
print(pipeline.last_trace.last_normalize_info)

# load again - no new files!
new_files = filesystem(bucket_url=TESTS_BUCKET_URL, file_glob="csv/*")
# add incremental on modification time
new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
load_info = pipeline.run((new_files | read_csv()).with_name("csv_files"))
print(load_info)
print(pipeline.last_trace.last_normalize_info)
```
Note how we use `apply_hints` to add incremental loading on **modification_date** to existing **new_files** and then
run it twice to demonstrate that files loaded in the first run are filtered out.

### Cleanup after loading
You can get **fsspec** client from **filesystem** resource after it was extracted ie. in order to delete processed files etc. The filesystem module contains convenience
method `fsspec_from_resource` that can be used as follows:
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