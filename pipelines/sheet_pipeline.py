import dlt
from google_sheets.google_sheets import google_spreadsheet

SPREADSHEET_ID = "1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU"

# FULL PIPELINE RUN
pipeline = dlt.pipeline(destination="postgres", full_refresh=False, dataset_name="sample_google_sheet_data")
data = google_spreadsheet(SPREADSHEET_ID, ["sheet1", "sheet2", "sheet3"])
info = pipeline.run(data)
print(info)
