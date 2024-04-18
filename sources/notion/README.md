# Notion

Notion is a tool that allows users to organize and manage their personal and professional lives. Using this Notion `dlt` verified source and pipeline example, you can load the databases from Notion to a [destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) of your choice. You can load the following source using this pipeline example:

| Source | Description |
| --- | --- |
| Notion_databases | feature that allows you to create structured collections of information. They are similar to spreadsheets or tables but with added flexibility and functionality. |

## Setup verified source and pipeline example

To grab the Notion credentials, and initialize the pipeline, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/notion)

## Initialize the verified source and pipeline example
```bash
dlt init notion bigquery
```

Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## **Add credentials**

1. Open `.dlt/secrets.toml`.
2. Enter the API key:
    ```toml
    # Put your secret values and credentials here
    # Note: Do not share this file and do not push it to GitHub!
    [source.notion]
    api_key = "set me up!" # Notion API token (e.g. secret_XXX...)
    ```
3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Run the pipeline example
1. Install the necessary dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```
    
2. Now the pipeline can be run by using the command:
    ```bash
    python3 notion_pipeline.py
    ```
    
3. To make sure that everything is loaded as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    
    For example, the pipeline_name for the above pipeline example is `notion`, you may also use any custom name instead.
    


💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT Notion verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT Notion documentation in [Setup Guide: Notion.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/notion)
