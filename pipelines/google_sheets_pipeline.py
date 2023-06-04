import dlt
from google_sheets import google_spreadsheet


def load_pipeline_with_ranges() -> None:
    """
    Does a full pipeline run. Will load all ranges in config.toml.  The dlt config also contains the spreadsheet url or id that data will be loaded from.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="test",
    )
    data = google_spreadsheet(get_sheets=False, get_named_ranges=False)
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_sheets() -> None:
    """
    Does a pipeline run. Will load all the sheets in the spreadsheet, but it will not load any of the named ranges in the spreadsheet. Will also load all the ranges given in config.
    The dlt config also contains the spreadsheet url or id that data will be loaded from.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(get_sheets=True, get_named_ranges=False)
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_named_ranges() -> None:
    """
    Does a pipeline run. Will not load the sheets in the spreadsheet, but it will load all the named ranges in the spreadsheet. Will also load all the ranges given in config.
    The dlt config also contains the spreadsheet url or id that data will be loaded from.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(get_sheets=False, get_named_ranges=True)
    info = pipeline.run(data)
    print(info)


def load_pipeline_with_sheets_and_ranges() -> None:
    """
    Does a pipeline run. Will load all the sheets in the spreadsheet and all the named ranges in the spreadsheet. Will also load all the ranges given in config.
    The dlt config also contains the spreadsheet url or id that data will be loaded from.
    """
    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination="postgres",
        full_refresh=False,
        dataset_name="sample_google_sheet_data",
    )
    data = google_spreadsheet(get_sheets=True, get_named_ranges=True)
    info = pipeline.run(data)
    print(info)


if __name__ == "__main__":
    load_pipeline_with_ranges()
