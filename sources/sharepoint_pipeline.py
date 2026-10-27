"""Loads SharePoint lists and the files of a document library folder into DuckDB."""

import dlt
from sharepoint import sharepoint_files, sharepoint_list, sharepoint_lists


def load_list() -> None:
    """Loads the list set in [sources.sharepoint.sharepoint_list_config]."""
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_to_duckdb",
        destination="duckdb",
        dataset_name="sharepoint_data",
    )
    load_info = pipeline.run(sharepoint_list())
    print(load_info)


def load_lists() -> None:
    """Loads every visible user-created list of the site, or the lists set in [sources.sharepoint.sharepoint_lists_config]."""
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_to_duckdb",
        destination="duckdb",
        dataset_name="sharepoint_data",
    )
    load_info = pipeline.run(sharepoint_lists())
    print(load_info)


def load_files() -> None:
    """Loads the files set in [sources.sharepoint.sharepoint_files_config]."""
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_to_duckdb",
        destination="duckdb",
        dataset_name="sharepoint_data",
    )
    load_info = pipeline.run(sharepoint_files())
    print(load_info)


if __name__ == "__main__":
    # Config is read from .dlt/config.toml and credentials from .dlt/secrets.toml.
    load_list()
    load_files()
