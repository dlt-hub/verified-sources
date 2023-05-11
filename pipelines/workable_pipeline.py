import dlt
from workable import workable_source, workable_jobs_with_details, workable_incremental_candidates_with_details


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


def load_job_with_details():
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_jobs",
    )
    job_with_details = workable_jobs_with_details()
    # run the pipeline with your parameters
    load_info = pipeline.run(job_with_details)
    # pretty print the information on data that was loaded
    print(load_info)


def load_candidates_with_details():
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_candidates",
    )
    candidates_with_details = workable_incremental_candidates_with_details()
    # run the pipeline with your parameters
    load_info = pipeline.run(candidates_with_details)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # load_all_data()
    # load_job_with_details()
    load_candidates_with_details()

