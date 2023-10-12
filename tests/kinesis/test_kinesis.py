import os
from typing import Any, Dict, List

import json
import dlt
from dlt.common import pendulum

from sources.kinesis import read_kinesis_stream
from tests.utils import assert_load_info, load_table_counts


def test_new_stream() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="telemetry_pipeline",
        destination="duckdb",
        dataset_name="dlt_telemetry",
    )

    stream_name = "dlt_ci_kinesis_source"

    # Create a kinesis stream and put some records in it
    for i in range(10):
        # create a data payload
        data = {'key': f'value_{i}'}
        data_str = json.dumps(data)

        # put the record to the stream
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=data_str,
            PartitionKey='tests_partition_key'
        )

    @dlt.transformer
    def segment_stream(items):
        yield items

    info = pipeline.run(
        read_kinesis_stream(
            stream_name,
            last_ts=dlt.sources.incremental(
                "_kinesis_ts", initial_value=pendulum.now().subtract(days=1)
            ),
        )
        | segment_stream
    )

    # Load entire database
    assert_load_info(info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    expected_tables = []
    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["sessions"] == 1
