import dlt
from google_sheets.google_sheets import google_spreadsheet

# constants
SPREADSHEET_ID = ""
SPREADSHEET_URL = ""

# FULL PIPELINE RUN
if __name__ == "main":
    pipeline = dlt.pipeline(pipeline_name="google_sheets_pipeline", destination="bigquery", full_refresh=False, dataset_name="sample_google_sheet_data")
    data = google_spreadsheet(spreadsheet_identifier=SPREADSHEET_ID)
    info = pipeline.run(data)
    print(info)
