# Mux

[Mux.com](http://mux.com/) is a video technology platform that provides infrastructure and tools for developers to build and stream high-quality video content. Using this Mux `dlt` verified source and pipeline example, you can load metadata about the assets and every video view to be loaded to the destination of your choice.
Using this verified source you can load data about the following resources:
| Resource | Description |
| --- | --- |
| Asset | Refers to the video content that you want to upload, encode, store, and stream using their platform |
| Video view | Represents a single instance of a video being watched or streamed |

## Initialize the Mux verified source and the pipeline example
```bash
dlt init mux bigquery
```

Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Grab Mux credentials

To grab the Mux credentials and initialize the pipeline, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mux)

## **Add credentials**

1. Open `.dlt/secrets.toml`.
2. Replace the API access and secret key with the appropriate credentials that you have retrieved.
    ```toml
    # Put your secret values and credentials here. Do not share this file and do not push it to github
    [sources.mux]
    mux_api_access_token = "please set me up" # Mux API access token
    mux_api_secret_key = "please set me up!" # Mux API secret key
    ```
    
3. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```
    
2. Now the verified source can be run by using the command:
    ```bash
    python3 mux_pipeline.py
    ```
    
3. To make sure that everything is loaded as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    
    For example, the pipeline_name for the above pipeline example is `mux`, you may also use any custom name instead.
    


💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT Mux documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT Mux documentation in [Setup Guide: Mux.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mux)

