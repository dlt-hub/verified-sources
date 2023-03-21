""" Loads the pipeline for Google Analytics V4. """

import time
import dlt
from google_analytics import google_analytics


def load_all(destination_name: str = "postgres"):
    """

    :param destination_name: 3 options: postgres, bigquery, redshift
    :return:
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(pipeline_name="dlt_google_analytics_pipeline", destination=destination_name, full_refresh=False, dataset_name="sample_analytics_data")
    # Google Analytics source function
    data_analytics = google_analytics()
    info = pipeline.run(data=data_analytics)
    return info


def incremental_load():
    """
    Loads data incrementally
    """
    pass


if __name__ == "__main__":
    start_time = time.time()
    load_all()
    end_time = time.time()
    print(f"Time taken: {end_time-start_time}")
