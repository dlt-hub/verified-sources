---
title: Filesystem
description: dlt source for fsspec filesystems
keywords: [filesystem, fsspec, s3]
---

# Filesystem Source

This source provides functionalities get data from fsspec filesystem. It supports all the filesystems supported by fsspec. For more information about fsspec, please visit [fsspec documentation](https://filesystem-spec.readthedocs.io/en/latest/index.html).

## Initialize the source

Initialize the source with dlt command:

```shell
dlt init filesystem duckdb
```

## Set filesystem credentials

1. Open `.dlt/secrets.toml`.
1. Enter the email account secrets:
   ```toml
   [sources.filesystem.credentials]
   bucket_url="~/Documents/csv_files/"
   ```

## Usage

TODO: add usage examples

## Functionality

TODO: add functionality description

## Example

TODO: add example
