import pytest
from unittest import mock

import pendulum

import dlt
from sources.csv_reader import csv_reader
from tests.utils import assert_load_info, assert_query_data, load_table_counts


TESTS_BUCKET_URLS = [
    ("file:///home/ilya/test_files/", ("*csv_reader_test*.csv",)),
]


@pytest.mark.parametrize("globs", TESTS_BUCKET_URLS)
def test_extract_data(globs):
    bucket_url = globs[0]
    globs = globs[1]

    pipeline = dlt.pipeline(
        pipeline_name="csv_to_duckdb",
        destination="postgres",
        dataset_name="files",
    )

    res = csv_reader(bucket_url, globs)
    load_info = pipeline.run(res)

    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts["read_location"] == 3

    assert_query_data(
        pipeline, f"SELECT col1 FROM read_location ORDER BY col1", (1, 2, 3)
    )

    assert_query_data(
        pipeline, f"SELECT col2 FROM read_location ORDER BY col1", ("yes", "yes", "no")
    )

    assert_query_data(
        pipeline, f"SELECT col3 FROM read_location ORDER BY col1", (3, 66, 8)
    )


@pytest.mark.parametrize("globs", TESTS_BUCKET_URLS)
def test_extract_incremental(globs):
    bucket_url = globs[0]
    globs = globs[1]

    pipeline = dlt.pipeline(
        pipeline_name="csv_to_duckdb",
        destination="postgres",
        dataset_name="files",
    )

    res = csv_reader(bucket_url, globs)

    with mock.patch(
        "dlt.current.resource_state",
        return_value={
            "last_modified": pendulum.datetime(2024, 1, 17, 9, 24, 47),
        },
    ):
        load_info = pipeline.run(res)

    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts["read_location"] == 2

    assert_query_data(
        pipeline, f"SELECT col2 FROM read_location ORDER BY col1", ("yes", "no")
    )

    assert_query_data(
        pipeline, f"SELECT col3 FROM read_location ORDER BY col1", (66, 8)
    )
