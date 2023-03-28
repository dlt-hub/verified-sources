import dlt
from google_sheets import google_spreadsheet


def full_pipeline_run() -> None:
    """
    Does a full pipeline run. Will load all the sheets in the spreadsheet and all the named ranges in the spreadsheet. Will also load all the ranges given in config.
    The dlt config also contains the spreadsheet url or id that data will be loaded from.
    """
    pipeline = dlt.pipeline(pipeline_name="google_sheets_pipeline", destination="postgres", full_refresh=True, dataset_name="sample_google_sheet_data")
    data = google_spreadsheet(get_sheets=True, get_named_ranges=True)
    info = pipeline.run(data)
    print(info)
    return


# FULL PIPELINE RUN
if __name__ == "__main__":
    full_pipeline_run()
