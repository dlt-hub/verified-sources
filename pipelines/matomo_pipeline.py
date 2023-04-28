"""Contains functions that run the matomo pipeline."""
from matomo import matomo_reports, matomo_events
import dlt


def run_full_load() -> None:
    """
    Does a basic run of the pipeline.
    """
    pipeline_reports = dlt.pipeline(dataset_name="matomo_full_load", export_schema_path="schemas/export", full_refresh=False, destination="postgres", pipeline_name="matomo")
    data_reports = matomo_reports()
    data_events = matomo_events()
    info = pipeline_reports.run([data_reports, data_events])
    print(info)


def run_custom_reports():
    """
    Defines some custom reports you can use and shows how to use for different custom reports
    :return:
    """

    queries = [
        {"resource_name": "custom_report_name",
         "methods": ["CustomReports.getCustomReport"],
         "date": "2020-01-01",
         "period": "day",
         "extra_params": {"idCustomReport": 1}},
        {"resource_name": "custom_report_name2",
         "methods": ["CustomReports.getCustomReport"],
         "date": "2020-01-01",
         "period": "day",
         "extra_params": {"idCustomReport": 2}},
    ]
    site_id = 3
    pipeline_reports = dlt.pipeline(dataset_name="matomo_custom_reports", full_refresh=False, destination="postgres", pipeline_name="matomo")
    data = matomo_reports(queries=queries, site_id=site_id)
    info = pipeline_reports.run(data)
    print(info)


def run_reports():
    """
    Runs the pipeline only loading reports.
    :return:
    """

    # site id can also be assigned explicitly. Default is to read from config.toml
    site_id = 3
    pipeline_reports = dlt.pipeline(dataset_name="matomo_reports", full_refresh=False, destination="postgres", pipeline_name="matomo")
    data = matomo_reports(site_id=site_id)
    info = pipeline_reports.run(data)
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
    run_full_load()
