import dlt
from google_sheets import google_spreadsheet


def full_pipeline_run(destination: str = "postgres", pipeline_name: str = "google_sheets_pipeline", full_refresh: bool = False, dataset_name: str = "sample_google_sheet_data") -> None:
    """
    Does a full pipeline run. Will load all the sheets in the spreadsheet and all the named ranges in the spreadsheet. Will also load all the ranges given in config.
    The dlt config also contains the spreadsheet url or id that data will be loaded from.
    :@param destination: The destination for the load: bigquery, postgres, or redshift
    @:param pipeline_name: The name of the pipeline
    @:param full_refresh: Setting that enables running the pipeline and losing all the previous saved context.
    """
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination=destination, full_refresh=full_refresh, dataset_name=dataset_name)
    data = google_spreadsheet(get_sheets=True, get_named_ranges=False)
    info = pipeline.run(data)
    print(info)
    return


# FULL PIPELINE RUN
if __name__ == "__main__":
    full_pipeline_run(dataset_name="test2", full_refresh=True)
