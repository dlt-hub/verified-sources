import dlt
from workable import workable_incremental, workable_source, workable_data_from_jobs, workable_data_from_candidates


def load_all_data(
    endpoints: tuple = ("members", "jobs", "stages", "candidates", "events")
):
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_all_data",
    )
    load_data = workable_source(endpoints=endpoints)
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)


def load_incremental_candidates():
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_incremental",
    )
    candidates = workable_incremental()
    # run the pipeline with your parameters
    load_info = pipeline.run(candidates)
    # pretty print the information on data that was loaded
    print(load_info)


def load_job_details():
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_jobs",
    )
    job_details = workable_data_from_jobs()
    # run the pipeline with your parameters
    load_info = pipeline.run(job_details)
    # pretty print the information on data that was loaded
    print(load_info)


def load_candidates_details():
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_candidates",
    )
    candidates_details = workable_data_from_candidates()
    # run the pipeline with your parameters
    load_info = pipeline.run(candidates_details)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # load_all_data()
    # load_incremental_candidates()
    # load_job_details()
    load_candidates_details()

