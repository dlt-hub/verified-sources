import dlt
from google_sheets import google_spreadsheet


def load_pipeline_with_ranges() -> None:
    """
    Loads explicitly passed ranges
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        full_refresh=False,
        dataset_name="test",
    )
    data = google_spreadsheet(
        "https://docs.google.com/spreadsheets/d/1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580/edit#gid=0",
        range_names=["hidden_columns_merged_cells", "Blank Columns"],
        # range_names=["NamedRange1", "Sheet 1", "Sheet 1!A1:D4"],
        get_sheets=False,
        get_named_ranges=False,
    )
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_sheets() -> None:
    """
    Does a pipeline run. Will load all the sheets in the spreadsheet, but it will not load any of the named ranges in the spreadsheet.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        full_refresh=False,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(
        "https://docs.google.com/spreadsheets/d/1BcG3BxKpe_v2kdIPdHE83yqG4oYUUZmL3Bc_rTZ59M4",
        # "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        # "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=0",
        get_sheets=True,
        get_named_ranges=False,
    )
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_named_ranges() -> None:
    """
    Does a pipeline run. Will not load the sheets in the spreadsheet, but it will load all the named ranges in the spreadsheet.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        full_refresh=False,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        get_sheets=False,
        get_named_ranges=True,
    )
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_sheets_and_ranges() -> None:
    """
    Does a pipeline run. Will load all the sheets in the spreadsheet and all the named ranges in the spreadsheet.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        full_refresh=True,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        get_sheets=True,
        get_named_ranges=True,
    )
    info = pipeline.run(data)
    print(info)


def load_with_table_rename_and_multiple_spreadsheets() -> None:
    """Demonstrates how to load two spreadsheets in one pipeline and how to rename tables"""

    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="duckdb",
        full_refresh=False,
        dataset_name="sample_google_sheet_data",
    )

    # take data from spreadsheet 1
    data = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        range_names=["Sheet 1!A1:B10"],
        get_named_ranges=False,
    )

    # take data from spreadsheet 2
    data_2 = google_spreadsheet(
        "1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580",
        range_names=["Sheet 1!B1:C10"],
        get_named_ranges=False,
    )
    # apply the table name to the existing resource: the resource name is the name of the range
    data.resources["Sheet 1!A1:B10"].apply_hints(table_name="my_a1_data")
    data_2.resources["Sheet 1!B1:C10"].apply_hints(table_name="second_sheet_data")

    # load two spreadsheets
    info = pipeline.run([data, data_2])
    print(info)
    # yes the tables are there
    user_tables = pipeline.default_schema.data_tables()
    # check if table is there
    assert set([t["name"] for t in user_tables]) == {
        "my_a1_data",
        "second_sheet_data",
        "spreadsheet_info",
    }


if __name__ == "__main__":
    load_with_table_rename_and_multiple_spreadsheets()
