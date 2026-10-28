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
    tasks = sharepoint_list(
        "Tasks", table_name="task_status", included_columns=["Title", "Status"]
    )
    load_info = pipeline.run(tasks)
    print(load_info)


def load_files() -> None:
    """Loads every csv file of the default library, subfolders included, into one table."""
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint", destination="duckdb", dataset_name="sharepoint_data"
    )
    csv_files = sharepoint_files(
        "", "csv", table_name="csv_files", file_glob="**/*.csv"
    )
    load_info = pipeline.run(csv_files)
    print(load_info)


if __name__ == "__main__":
    load_lists()
    load_files()
    # load_list_columns()
