"""Contains functions that run the matomo pipeline."""
from matomo import matomo, matomo_live_data
import dlt
from time import time


def basic_pipeline_run() -> None:
    """
    Does a basic run of the pipeline.
    """
    pipeline = dlt.pipeline(dataset_name="matomo", full_refresh=False, destination="postgres", pipeline_name="matomo2")
    data = matomo()
    info = pipeline.run(data)
    print(info)


def run_custom_reports():
    """
    Defines some custom reports you can use and shows how to use for different custom reports
    :return:
    """
    pass


def run_live_reports():
    """
    Defines some live reports you can use and shows how to use for different live reports
    :return:
    """

    pipeline = dlt.pipeline(dataset_name="matomo_live", full_refresh=False, destination="postgres", pipeline_name="matomo2")
    data = matomo_live_data()
    info = pipeline.run(data)
    print(info)



def run_normal_reports():
    """
    Defines some normal reports you can use and shows how to use for different normal reports. Can also be defined
    :return:
    """
    pass


if __name__ == "__main__":
    start = time()
    run_live_reports()
    end = time()
    print(f"Time taken: {end-start}")
