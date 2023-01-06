import dlt
from google_sheets.google_sheets import google_spreadsheet, api_test

# dlt.pipeline(destination="bigquery", full_refresh=False)
# # see example.secrets.toml to where to put credentials
#
# info = google_spreadsheet("11G95oVZjieRhyGqtQMQqlqpxyvWkRXowKE8CtdLtFaU", ["2022-05", "model_metadata"]).run()
# print(info)


SPREADSHEET_ID = "1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU"


# SHEET API OUTPUT TESTING
#info = api_test(SPREADSHEET_ID)


# FULL PIPELINE RUN
pipeline = dlt.pipeline(destination="postgres", full_refresh=False, dataset_name="sample_google_sheet_data")
data = google_spreadsheet(SPREADSHEET_ID, ["sheet1", "sheet2", "sheet3"])
info = pipeline.run(data)
print(info)
