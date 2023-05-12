import dlt

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


if __name__ == "__main__":
    load_all_data()

