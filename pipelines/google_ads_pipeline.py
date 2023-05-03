import time
import dlt
from google_ads import google_ads, google_ads_query


def load_queries():
    """
    Loads all input queries
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(pipeline_name="dlt_google_ads_pipeline", destination="postgres", full_refresh=False, dataset_name="google_ads_queries")
    data_queries = google_ads_query()
    info = pipeline.run(data_queries)
    return info


def load_default():
    """
    Loads default tables for google ads
    """
    pipeline = dlt.pipeline(pipeline_name="dlt_google_ads_pipeline", destination="postgres", full_refresh=False, dataset_name="google_ads_default")
    data_default = google_ads()
    info = pipeline.run(data_default)
    return info


def load_all():
    """
    Loads custom queries and default tables
    """

    pipeline = dlt.pipeline(pipeline_name="dlt_google_ads_pipeline", destination="postgres", full_refresh=False, dataset_name="full_load_google_ads")
    data_default = google_ads()
    data_queries = google_ads_query()
    info = pipeline.run(data=[data_default, data_queries])
    return info


if __name__ == "__main__":
    load_info = load_all()
    print(load_info)

