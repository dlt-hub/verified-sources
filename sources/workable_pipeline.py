import dlt

from pendulum import datetime
from workable import workable_source


def load_all_data() -> None:
    """
    This demo script uses the resources with non-incremental
    loading based on "replace" mode to load all data from provided endpoints,
    and only one "candidate" resource that loads incrementally.
    """
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


def load_all_data_with_details() -> None:
    """
    This demo script uses the resources with non-incremental
    loading based on "replace" mode to load all data from provided endpoints,
    and only one "candidate" resource that loads incrementally.

    Additionally, data is loaded from endpoints that depend on the main endpoints.
    Example: /jobs/:shortcode/members, /candidates/:id/comments
    """
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


def load_data_by_date() -> None:
    """
    This demo script uses the resources with non-incremental
    loading based on "replace" mode to load all data from provided endpoints,
    and only one "candidate" resource that loads incrementally.

    All non-incremental data filtered by start_date.
    Incremental resource uses start_date as initial_value.
    It does not affect dependent resources (jobs_activities, candidates_activities, etc).
    """
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
    load_all_data_with_details()
    # load_data_by_date()
