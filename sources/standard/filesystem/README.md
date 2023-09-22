---
title: Filesystem
description: dlt source for fsspec filesystems
keywords: [filesystem, fsspec, s3]
---

# Filesystem Source

This source provides functionalities get data from fsspec filesystem. It supports all the
filesystems supported by fsspec. For more information about fsspec, please visit
[fsspec documentation](https://filesystem-spec.readthedocs.io/en/latest/index.html).

## Initialize the source

Initialize the source with dlt command:

```shell
dlt init filesystem duckdb
```

## Set filesystem credentials

1. Open `.dlt/secrets.toml`.
2. Add the bucket url which contains the path to the files, if you are using a local filesystem,
   you can use the `file://` prefix or just ignore the prefix. For example:
   ```toml
   [sources.filesystem]
   bucket_url="~/Documents/csv_files/"
   ```
   For remote filesystems you need to add the prefix, it will be used to get the protocol bein
   used, for example:
   ```toml
   [sources.filesystem]
   bucket_url="s3://my-bucket/csv_files/"
   ```
3. If your filesystem requires credentials, you can add them in the `credentials` section, for
   example:
   ```toml
   [sources.filesystem.credentials]
   aws_access_key_id="..."
   aws_secret_access_key="..."

   ```

## Usage

The filesystem resource can be used to get data from a filesystem. It should be used with a
transformer that will handle and process the files, in the `standard_pipeline.py` file you can find
some examples of how you can handle the files. The example bellow implement a transformer that will
copy the files to a local directory.

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

- `extract_content`: If set to `True`, the content of the file will be read and returned in the
  resource. Defaults to `False`.
- `chunksize`: The number of files that will be read at once. This is useful to avoid reading all
  the files at once, which can be a problem if you have a lot of files or if the files are big and
  you are using the `extract_content` parameter.

The returned data item will also provide some methods to read the file:
- `read`: Reads the file and returns the content as bytes.
- `open`: Opens the file and returns a file object.
  
When using the `extract_content` parameter, the `read` and `open` methods will not read the remote
filesystem, and get the content from the data item.
