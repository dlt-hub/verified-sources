import dlt
from google_sheets.google_sheets import google_spreadsheet

# constants
SPREADSHEET_ID = "1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU/edit#gid=0"
RANGES = ["sheet1", "sheet2", "sheet3!A1:I11", "sheet3!G18:O28"]

# FULL PIPELINE RUN
pipeline = dlt.pipeline(destination="postgres", full_refresh=True, dataset_name="sample_google_sheet_data")
data = google_spreadsheet(spreadsheet_identifier=SPREADSHEET_URL, sheet_names=RANGES)
info = pipeline.run(data)
print(info)
