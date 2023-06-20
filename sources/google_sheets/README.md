# Google Sheets

This verified source can be used to load data from a [Google Sheets](https://www.google.com/sheets/about/) workspace onto a destination of your choice.

| Endpoints | Description |
| --- | --- |
| Tables | tables of the spreadsheet, tables have same name as individual sheets |
| Named ranges | loaded as a separate column with an automatically generated header. |
| Merged cells | retains only the cell value that was taken during the merge (e.g., top-leftmost), and every other cell in the merge is given a null value. |


Initialize a `dlt` project with the following command:
```bash
dlt init google_sheets bigquery
```

Here, we chose BigQuery as the destination. To choose a different destination, replace `bigquery` with your choice of [destination.](https://dlthub.com/docs/dlt-ecosystem/destinations)

## Grab Google Sheets credentials

To read about grabbing the Google Sheets credentials and configuring the verified source, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_sheets#google-sheets-api-authentication)

## Add credentials

1. Open `.dlt/secrets.toml`
2. From the .json that you downloaded earlier, copy “project_id”, “private_key”, and “client_email” as follows:
    
    ```toml
    [sources.google_spreadsheet.credentials]
    project_id = "set me up" # GCP Source project ID!
    private_key = "set me up" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)
    client_email = "set me up" # Email for source service account
    location = "set me up" #Project Location For ex. “US”
    
    ```
    
3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Run the pipeline

1. Install the requirements by using the following command
    
    ```bash
    pip install -r requirements.txt
    ```
    
2. Run the pipeline by using the following command
    
    ```bash
    python3 google_sheets_pipelines.py
    ```
    
3. Use the following command to make sure that everything loaded as expected.
    
    ```bash
    dlt pipeline google_sheets_pipeline show
    ```
    


💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT Google Sheets documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT Google Sheets documentation in [Setup Guide: Google Sheets](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_sheets).

