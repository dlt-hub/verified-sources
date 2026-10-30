"""Loads SharePoint lists and the files of a document library into DuckDB."""

import dlt
from sharepoint import sharepoint_files, sharepoint_list, sharepoint_lists


def load_lists() -> None:
    """Loads every visible generic list of the site, one table per list."""
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint", destination="duckdb", dataset_name="sharepoint_data"
    )
    load_info = pipeline.run(sharepoint_lists())
    print(load_info)


def load_list_columns() -> None:
    """Loads selected columns of one list into its own table."""
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint", destination="duckdb", dataset_name="sharepoint_data"
    )
    tasks = sharepoint_list("Tasks", included_columns=["Title", "Status"])
    load_info = pipeline.run(tasks.with_name("task_status"))
    print(load_info)


def load_files() -> None:
    """Loads the csv files of one folder of the default library into one table."""
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint", destination="duckdb", dataset_name="sharepoint_data"
    )
    reports = sharepoint_files(
        "Reports", "csv", table_name="reports", file_glob="*.csv"
    )
    load_info = pipeline.run(reports)
    print(load_info)


if __name__ == "__main__":
    load_lists()
    # these need a list and a folder of your site
    # load_list_columns()
    # load_files()
