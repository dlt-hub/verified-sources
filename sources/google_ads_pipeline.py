import dlt
from dlt.common.pipeline import LoadInfo
from google_ads import google_ads


def load_pipeline() -> LoadInfo:
    """
    Loads custom queries and default tables
    """

    pipeline = dlt.pipeline(
        pipeline_name="dlt_google_ads_pipeline",
        destination="duckdb",
        dev_mode=False,
        dataset_name="full_load_google_ads",
    )
    data_default = google_ads()
    info = pipeline.run(data=[data_default])
    return info


if __name__ == "__main__":
    load_info = load_pipeline()
    print(load_info)
