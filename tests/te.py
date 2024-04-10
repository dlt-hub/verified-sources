import os
import posixpath
from typing import Iterator
import dlt
from dlt.sources import TDataItems

from sources.filesystem import (
    FileItemDict,
    filesystem,
    readers,
    read_csv,
)

TESTS_BUCKET_URL = r"\\localhost\\c$\\git_reps"

# import fsspec

# with fsspec.open(TESTS_BUCKET_URL + "\\VendorProvince.csv") as f:
#     print(f.read())

# exit()


def test_load_csv() -> None:
    """Demonstrates how to scan folder with csv files, load them in chunk and merge on date column with the previous load"""
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem_csv",
        destination="duckdb",
        dataset_name="file_data",
        full_refresh=True,
    )

    data_file = (
        filesystem(bucket_url=TESTS_BUCKET_URL, file_glob="VendorProvince.csv")
        | read_csv()
    )

    load_info = pipeline.run(data_file)

    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


test_load_csv()
# //localhost\c$\git_reps/VendorProvince.csv
