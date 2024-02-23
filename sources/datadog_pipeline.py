import dlt
from dlt.sources.helpers import requests


from datadog import datadog_source


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="datadog",
        destination="duckdb",
        dataset_name="datadog_data",
    )
    load_info = pipeline.run(
        datadog_source().with_resources("authentication"),
        table_name="test",
    )
    print(load_info)

    # run the pipeline with your parameters
    load_info = pipeline.run(
        datadog_source().with_resources("monitors"),
        table_name="monitors",
    )
    print(load_info)

    load_info = pipeline.run(
        datadog_source().with_resources("slos"),
        table_name="slos",
    )
    print(load_info)

    load_info = pipeline.run(
        datadog_source().with_resources("synthetics"),
        table_name="synthetics",
    )
    print(load_info)

    load_info = pipeline.run(
        datadog_source().with_resources("synthetic_tests"),
        table_name="synthetic_tests",
    )
    print(load_info)
