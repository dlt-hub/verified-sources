""" Loads the pipeline for Google Analytics V4. """

import time
import dlt
from google_analytics import google_analytics


# this can also be filled in config.toml and be left empty as a parameter.
QUERIES = [
    {
        "resource_name": "sample_analytics_data1",
        "dimensions": ["browser", "city"],
        "metrics": ["totalUsers", "transactions"],
    },
    {
        "resource_name": "sample_analytics_data2",
        "dimensions": ["browser", "city", "dateHour"],
        "metrics": ["totalUsers"],
    },
]


def simple_load():
    """
    Just loads the data normally. Incremental loading for this pipeline is on,
    the last load time is saved in dlt_state, and the next load of the pipeline will have the last load as a starting date.

    Returns:
        Load info on the pipeline that has been run.
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(
        pipeline_name="dlt_google_analytics_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_analytics_data",
    )
    # Google Analytics source function - taking data from QUERIES defined locally instead of config
    # TODO: pass your google analytics property id
    data_analytics = google_analytics(property_id=0, queries=QUERIES)
    info = pipeline.run(data=data_analytics)
    print(info)
    return info


def simple_load_config():
    """
    Just loads the data normally. QUERIES are taken from config. Incremental loading for this pipeline is on,
    the last load time is saved in dlt_state, and the next load of the pipeline will have the last load as a starting date.

    Returns:
        Load info on the pipeline that has been run.
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(
        pipeline_name="dlt_google_analytics_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_analytics_data",
    )
    # Google Analytics source function - taking data from QUERIES defined locally instead of config
    data_analytics = google_analytics()
    info = pipeline.run(data=data_analytics)
    print(info)
    return info


def chose_date_first_load(start_date: str = "2000-01-01"):
    """
    Chooses the starting date for the first pipeline load. Subsequent loads of the pipeline will be from the last loaded date.

    Args:
        start_date: The string version of the date in the format yyyy-mm-dd and some other values.
            More info: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange

    Returns:
        Load info on the pipeline that has been run.
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(
        pipeline_name="dlt_google_analytics_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_analytics_data",
    )
    # Google Analytics source function
    data_analytics = google_analytics(start_date=start_date)
    info = pipeline.run(data=data_analytics)
    print(info)
    return info


if __name__ == "__main__":
    start_time = time.time()
    simple_load()
    end_time = time.time()
    print(f"Time taken: {end_time-start_time}")
