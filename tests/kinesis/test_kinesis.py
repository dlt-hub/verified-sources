import json
import uuid

import dlt
from dlt.common import pendulum
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.typing import TDataItem

from sources.kinesis import read_kinesis_stream
from tests.utils import assert_load_info, load_table_counts


def test_kinesis() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="kinesis_test",
        destination="duckdb",
        dataset_name="kinesis_test_data",
    )

    stream_name = "dlt_ci_kinesis_source"
    test_id = str(uuid.uuid4())

    credentials = dlt.secrets._get_value("sources.kinesis_pipeline.credentials")[0]
    aws_credentials = AwsCredentials(credentials)

    session = aws_credentials._to_botocore_session()
    kinesis_client = session.create_client("kinesis")

    # A binary record
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=f"binary_stream data {test_id}".encode(),
        PartitionKey="binary_partition",
    )

    # A string record
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=f"sting_stream data {test_id}",
        PartitionKey="string_partition",
    )

    # Some json records
    records = []
    for i in range(10):
        data = {"key": f"{test_id}_{i}"}
        data_str = json.dumps(data).encode()
        record = {
            "Data": data_str,
            "PartitionKey": "json_partition",
        }
        records.append(record)

    kinesis_client.put_records(
        StreamName=stream_name,
        Records=records,
    )

    @dlt.transformer(name="kinesis_records")
    def filter_test_records(items: TDataItem, test_id: str) -> TDataItem:
        filtered = []
        for item in items:
            if item["_kinesis_partition"] == "json_partition":
                if test_id.encode() in item["data"]:
                    filtered.append(item)
            elif item["_kinesis_partition"] == "string_partition":
                if item["data"].endswith(test_id.encode()):
                    filtered.append(item)
            elif item["_kinesis_partition"] == "binary_partition":
                if item["data"].endswith(test_id.encode()):
                    filtered.append(item)
        yield filtered

    kinesis_resource = read_kinesis_stream(
        stream_name,
        last_ts=dlt.sources.incremental(
            "_kinesis_ts", initial_value=pendulum.now().subtract(seconds=30)
        ),
    ) | filter_test_records(test_id=test_id)

    load_info = pipeline.run(kinesis_resource)

    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    expected_tables = ["kinesis_records"]
    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["kinesis_records"] == 12

    # run once more to check if the incremental load works
    load_info = pipeline.run(kinesis_resource)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    expected_tables = ["kinesis_records"]
    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["kinesis_records"] == 12
