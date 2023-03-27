""" Loads the pipeline for Google Analytics V4. """

import time
import dlt
from google_analytics import google_analytics

queries = [
    {"resource_name": "sample_analytics_data1", "dimensions": ["browser", "city"], "metrics": ["totalUsers", "transactions"]},
    {"resource_name": "sample_analytics_data2", "dimensions": ["browser", "city"], "metrics": ["totalUsers"]}
]


def simple_load(destination_name: str = "postgres"):
    """
    Just loads the data normally. Incremental loading for this pipeline is on, the last load time is saved in dlt_state and the next load of the pipeline will have the last load as a starting date.
    :param destination_name: 3 options: postgres, bigquery, redshift
    :returns: Load info on the pipeline that has been run
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(pipeline_name="dlt_google_analytics_pipeline", destination=destination_name, full_refresh=False, dataset_name="sample_analytics_data")
    # Google Analytics source function
    data_analytics = google_analytics(queries=queries)
    info = pipeline.run(data=data_analytics)
    return info


def chose_date_load(destination_name: str = "postgres", start_date: str = "2000-01-01", end_date: str = "today"):
    """
    Chooses the starting and end date for the pipeline load.
    :param destination_name: 3 options: postgres, bigquery, redshift
    :param start_date: Needs to be the string version of date in the format yyyy-mm-dd and some other values: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange
    :param end_date: Needs to be the string version of date in the format yyyy-mm-dd and some other values: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange
    :returns: Load info on the pipeline that has been run
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(pipeline_name="dlt_google_analytics_pipeline", destination=destination_name, full_refresh=False, dataset_name="sample_analytics_data")
    # Google Analytics source function
    data_analytics = google_analytics(queries=queries, start_date=start_date, end_date=end_date)
    info = pipeline.run(data=data_analytics)
    return info


if __name__ == "__main__":
    start_time = time.time()
    simple_load()
    end_time = time.time()
    print(f"Time taken: {end_time-start_time}")
