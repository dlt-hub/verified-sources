from typing import Sequence

import dlt

from google_sheets import google_spreadsheet


def load_pipeline_with_ranges(
    spreadsheet_url_or_id: str, range_names: Sequence[str]
) -> None:
    """
    Loads explicitly passed ranges
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        dev_mode=True,
        dataset_name="test",
    )
    data = google_spreadsheet(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        range_names=range_names,
        get_sheets=False,
        get_named_ranges=False,
    )
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_sheets(spreadsheet_url_or_id: str) -> None:
    """
    Will load all the sheets in the spreadsheet, but it will not load any of the named ranges in the spreadsheet.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        dev_mode=True,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        get_sheets=True,
        get_named_ranges=False,
    )
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_named_ranges(spreadsheet_url_or_id: str) -> None:
    """
    Will not load the sheets in the spreadsheet, but it will load all the named ranges in the spreadsheet.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        dev_mode=True,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        get_sheets=False,
        get_named_ranges=True,
    )
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_sheets_and_ranges(spreadsheet_url_or_id: str) -> None:
    """
    Will load all the sheets in the spreadsheet and all the named ranges in the spreadsheet.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        dev_mode=True,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        get_sheets=True,
        get_named_ranges=True,
    )
    info = pipeline.run(data)
    print(info)


def load_with_table_rename_and_multiple_spreadsheets(
    spreadsheet_url_or_id: str, range_names: Sequence[str]
) -> None:
    """Demonstrates how to load two spreadsheets in one pipeline and how to rename tables"""

    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        dev_mode=True,
        dataset_name="sample_google_sheet_data",
    )

    # take data from spreadsheet 1
    data = google_spreadsheet(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        range_names=[range_names[0]],
        get_named_ranges=False,
    )

    # take data from spreadsheet 2
    data_2 = google_spreadsheet(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        range_names=[range_names[1]],
        get_named_ranges=False,
    )
    # apply the table name to the existing resource: the resource name is the name of the range
    data.resources[range_names[0]].apply_hints(table_name="first_sheet_data")
    data_2.resources[range_names[1]].apply_hints(table_name="second_sheet_data")

    # load two spreadsheets
    info = pipeline.run([data, data_2])
    print(info)
    # yes the tables are there
    user_tables = pipeline.default_schema.data_tables()
    # check if table is there
    assert {t["name"] for t in user_tables} == {
        "first_sheet_data",
        "second_sheet_data",
        "spreadsheet_info",
    }


if __name__ == "__main__":
    url_or_id = "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580"
    range_names = ["hidden_columns_merged_cells", "Blank Columns"]

    load_pipeline_with_ranges(url_or_id, range_names)
    load_pipeline_with_sheets(url_or_id)
    load_pipeline_with_named_ranges(url_or_id)
    load_pipeline_with_sheets_and_ranges(url_or_id)
    load_with_table_rename_and_multiple_spreadsheets(url_or_id, range_names)
