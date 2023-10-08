---
title: Filesystem
description: dlt source for fsspec filesystems
keywords: [filesystem, fsspec, s3]
---

# Filesystem Source

This [standalone resource]() lists files in s3, gcs and azure buckets and also supports
local file system and other **fsspec** compatible file systems. For more information about **fsspec**,
please visit [fsspec documentation](https://filesystem-spec.readthedocs.io/en/latest/index.html).

The filesystem source can list, open and read files that can be used with other transformers to
build pipelines that can serve various purposes. You can find examples of how to use this resource
in the `standard_pipeline.py` file.

## Initialize the source

Initialize the source with dlt command:

```shell
dlt init standard duckdb
```

## Set filesystem credentials

1. Open `.dlt/secrets.toml`.
2. Please read the [corresponding documentation]() on **filesystem** destination which explains the setup of all bucket types.
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
## Usage

The filesystem resource will list files in selected bucket using specified glob pattern and return information on each file (`FileInfo` below) in pages of configurable size.
The resource is designed to work with [transform functions]() and [transformers]() that should be used to build specialized extract pipelines ie.
* filter only files with given mime type
* copy the files locally
* [read file content and parse text out of PDF]()
* [stream the content]() of **csv, jsonl or parquet** files

Please refer to examples in [sources/standard_pipeline.py](../../standard_pipeline.py)

`filesystem` resource takes following parameters:
* **bucket_url**: An url to a bucket
* **credentials**: One of the bucket credentials (typically from config) or `AbstractFilesystem` (fsspec client) instance
* **extract_content**: If set to `True`, the content of the file will be read and returned in the
  resource. Defaults to `False`.
* **file_glob**: A glob pattern supported by [fsspec](). Defaults to `*` (all files from current folder, non recursive)
* **chunksize**: The number of files that will be read at once. This is useful to avoid reading all
  the files at once, which can be a problem if you have a lot of files or if the files are big and
  you are using the `extract_content` parameter.

**We recommend that you give each pipeline a [specific name]()** before loading with `pipeline.run`. This will make sure that data goes to a table with
the name you want and that each pipeline [uses a separate state for incremental loading]().

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
- **read_bytes(**: Reads the file and returns the content as **bytes**.
- **open(**: Opens the file and returns a file object.
And property
- **filesystem**: returns authorized `AbstractFilesystem` with all usual fsspec methods.

### Read csvs example


### Incremental loading
You can convert `filesystem` resource into incremental one. It already defines `primary_key` on `file_url` and each `FileItem` contains `modification_time`. Following example will return only files that were modified/created from the previous run:
```python
```

### Cleanup after loading
You can get **fsspec** client from **filesystem** resource after it was extracted ie. in order to delete processed files etc. The standard module contains convenience
method `fsspec_from_resource` that can be used as follows:
```python
from standard import filesystem, fsspec_from_resource
# get filesystem source
gs_resource = filesystem("gs://ci-test-bucket/")
# extract files
pipeline.run(gs_resource | read_csv)
# get fs client
fs_client = fsspec_from_resource(gs_resource)
fs_client.ls("ci-test-bucket/standard_source/samples")
```


## Example

```python
import os
import dlt
from sources.standard.filesystem import filesystem_resource

@dlt.transformer
def copy_files(
   items: TDataItems,
   storage_path: str,
) -> TDataItem:
   """Reads files and copy them to local directory."""
   storage_path = os.path.abspath(storage_path)
   os.makedirs(storage_path, exist_ok=True)
   for file_obj in items:
      file_dst = os.path.join(storage_path, file_obj["file_name"])
      file_obj["path"] = file_dst
      with open(file_dst, "wb") as f:
            f.write(file_obj.read())
      yield file_obj

   file_source = filesystem_resource(chunksize=10) | copy_files(storage_path="standard/files")
```

The filesystem resource provides the following parameters:




