import dlt
import pendulum
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItem

try:
    from .kinesis import read_kinesis_stream  # type: ignore
except ImportError:
    from kinesis import read_kinesis_stream


def load_kinesis() -> LoadInfo:
    """Use the kinesis source to completely load all streams"""

    @dlt.transformer
    def segment_stream(items: TDataItem) -> TDataItem:
        yield items

    pipeline = dlt.pipeline(
        pipeline_name="telemetry_pipeline",
        destination="duckdb",
        dataset_name="dlt_telemetry",
    )

    info = pipeline.run(
        read_kinesis_stream(
            "dlt_ci_kinesis_source",
            last_ts=dlt.sources.incremental(
                "_kinesis_ts", initial_value=pendulum.now().subtract(days=1)
            ),
        )
        | segment_stream
    )
    if len(info.loads_ids) == 0:
        print("No messages in kinesis")
    return info


def test_kinesis() -> None:
    import json

    pipeline = dlt.pipeline(
        pipeline_name="telemetry_pipeline",
        destination="duckdb",
        dataset_name="dlt_telemetry",
    )

    # Creates the test data
    stream_name = "dlt_ci_kinesis_source"

    credentials = dlt.secrets._get_value("sources.kinesis_pipeline.credentials")[0]
    aws_credentials = AwsCredentials(credentials)

    session = aws_credentials._to_botocore_session()
    kinesis_client = session.create_client("kinesis")

    # A binary record
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=b"binary_stream data\n",
        PartitionKey="binary_partition",
    )

    # Some json records
    records = []
    for i in range(5):
        data = {"key": f"value_{i}"}
        data_str = json.dumps(data).encode()
        record = {
            "Data": data_str,
            "PartitionKey": "json_partition",
        }
        records.append(record)

    kinesis_client.put_records(
        StreamName=stream_name,
        Data=records,
    )

    @dlt.transformer
    def segment_stream(items: TDataItem) -> TDataItem:
        yield items

    pipeline.run(
        read_kinesis_stream(
            stream_name,
            last_ts=dlt.sources.incremental(
                "_kinesis_ts", initial_value=pendulum.now().subtract(seconds=2)
            ),
        )
        | segment_stream
    )


if __name__ == "__main__":
    # Credentials for the sample database.
    # Load selected tables with different settings
    # load_kinesis()
    test_kinesis()
