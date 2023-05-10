import dlt
from workable import workable_incremental, workable_jobs_activities, workable_source


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
        dataset_name="workable_candidates",
    )
    candidates = workable_incremental()
    # run the pipeline with your parameters
    load_info = pipeline.run(candidates)
    # pretty print the information on data that was loaded
    print(load_info)


def load_jobs_activities():
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination="duckdb",
        dataset_name="workable_activities",
    )
    job_activities = workable_jobs_activities()
    # run the pipeline with your parameters
    load_info = pipeline.run(job_activities)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # load_all_data()
    # load_incremental_candidates()
    load_jobs_activities()
