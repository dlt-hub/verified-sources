import dlt
import pendulum
from dlt.pipeline.pipeline import Pipeline
from dlt.common.pipeline import LoadInfo

# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .kinesis import read_kinesis_stream
except ImportError:
    from kinesis import read_kinesis_stream


def load_kinesis(pipeline: Pipeline = None) -> LoadInfo:
    """Use the kinesis source to completely load all streams"""

    @dlt.transformer
    def segment_stream(items):
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
    else:
        print(info)


if __name__ == "__main__":
    # Credentials for the sample database.
    # Load selected tables with different settings
    load_kinesis()


    # for i in range(200):
    #     # create a data payload
    #     data = {'key': f'value_{i}'}
    #     data_str = json.dumps(data)

    #     # put the record to the stream
    #     response = kinesis_client.put_record(
    #         StreamName=stream_name,
    #         Data=data_str,
    #         PartitionKey='tests_partition_key'
    #     )