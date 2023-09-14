# Filesystem Source

This source provides functionalities get data from fsspec filesystem.

## Prerequisites

- Python 3.x
- `dlt` library (you can install it using `pip install dlt`)
- destination dependencies, e.g. `duckdb` (`pip install duckdb`)

## Installation

Make sure you have Python 3.x installed on your system.

Install the required library by running the following command:

```shell
pip install dlt[duckdb]
```

## Initialize the source

Initialize the source with dlt command:

```shell
dlt init inbox duckdb
```

## Set email account credentials

1. Open `.dlt/secrets.toml`.
1. Enter the email account secrets:
   ```toml
   [sources.filesystem.credentials]
   bucket_url="/Users/josue/Documents/dlthub/verified-sources/sources/standard/csv_files/"
   ```

## Usage

TODO: add usage examples

## Functionality

TODO: add functionality description

## Example

TODO: add example
