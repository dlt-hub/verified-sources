import dlt
from dlt.pipeline.pipeline import Pipeline
from dlt.common.pipeline import LoadInfo

# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .kinesis import read_kinesis_stream
except ImportError:
    from kinesis import read_kinesis_stream


def load_kinesis(pipeline: Pipeline = None) -> LoadInfo:
    """Use the kinesis source to completely load all streams"""
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_kinesis",
            destination="duckdb",
            dataset_name="kinesis_database",
        )

    source = read_kinesis_stream()

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")

    return info


if __name__ == "__main__":
    # Credentials for the sample database.
    # Load selected tables with different settings
    print(load_kinesis())
