import dlt
from google_sheets.google_sheets import google_spreadsheet
import time


# constants
SPREADSHEET_ID = "1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU/edit#gid=0"

# FULL PIPELINE RUN
start = time.time()
pipeline = dlt.pipeline(destination="postgres", full_refresh=False, dataset_name="sample_google_sheet_data")
data = google_spreadsheet(spreadsheet_identifier=SPREADSHEET_URL)
info = pipeline.run(data)
end = time.time()
print(info)
print(f"Execution time: {end-start}")
