import dlt
from pendulum import datetime

from workable import (
    workable_source
)


def load_all_data(
):
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_all_data",
    )
    load_data = workable_source()
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)


def load_all_data_with_details(
):
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_all_data",
    )
    load_data = workable_source(load_details=True)
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)


def load_data_by_date(
):
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_all_data",
    )
    load_data = workable_source(start_date=datetime(2023, 2, 1), load_details=True)
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # load_all_data()
    # load_all_data_with_details()
    load_data_by_date()