"""Contains functions that run the matomo pipeline."""
from matomo import matomo_reports, matomo_events
import dlt
from time import time


def run_full_load() -> None:
    """
    Does a basic run of the pipeline.
    """
    pipeline_reports = dlt.pipeline(dataset_name="matomo_full_load", full_refresh=False, destination="postgres", pipeline_name="matomo")
    data_reports = matomo_reports()
    data_events = matomo_events()
    info = pipeline_reports.run([data_reports, data_events])
    print(info)


def run_custom_reports():
    """
    Defines some custom reports you can use and shows how to use for different custom reports
    :return:
    """
    pass


def run_reports():
    """
    Runs the pipeline only loading reports.
    :return:
    """
    pipeline_events = dlt.pipeline(dataset_name="matomo_reports", full_refresh=False, destination="postgres", pipeline_name="matomo")
    data = matomo_reports()
    info = pipeline_events.run(data)
    print(info)


def run_live_events():
    """
    Runs the pipeline only loading live events.
    :return:
    """

    pipeline_events = dlt.pipeline(dataset_name="matomo_events", full_refresh=False, destination="postgres", pipeline_name="matomo")
    data = matomo_events()
    info = pipeline_events.run(data)
    print(info)


if __name__ == "__main__":
    start = time()
    run_live_events()
    end = time()
    print(f"Time taken: {end-start}")
