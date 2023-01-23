import dlt
from google_sheets.google_sheets import google_spreadsheet
import time


# constants
SPREADSHEET_ID = "1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU/edit#gid=0"
RANGES = ["sheet3!G18:O28", "sheet1", "sheet2", "sheet3!A1:I11", "sheet4", "all_types"]
RANGES2 = ["all_types", "only_headers", "only_data", "has_empty", "inconsistent_types", "empty_row", "empty_rows", "two_tables", "more_headers_than_data"]

# FULL PIPELINE RUN
start = time.time()
pipeline = dlt.pipeline(destination="postgres", full_refresh=False, dataset_name="sample_google_sheet_data")
data = google_spreadsheet(spreadsheet_identifier=SPREADSHEET_URL)
info = pipeline.run(data)
end = time.time()
print(info)
print(f"Execution time: {end-start}")
