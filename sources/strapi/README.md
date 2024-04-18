# Strapi

> **Warning!**
>
> This source is a Community source, and we never tested it. Currently, we **don't test** it on a regular basis.
> If you have any problem with this source, ask for help in our [Slack Community](https://dlthub.com/community).


Strapi is a headless CMS (Content Management System) that allows developers to create powerful API-driven content management systems without having to write a lot of custom code.

Since the endpoints available in Strapi depend on your API setup created, you need to be aware of which endpoints you will be ingesting in order to get data from Strapi into your warehouse.

## Initialize the pipeline
```bash
dlt init strapi bigquery
```

Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source

To grab the API token and initialise the verified source, read the [following documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/strapi)

## Add credentials
1. Open `.dlt/secrets.toml`.
2. Enter the api_secret_key:
    ```toml
    # put your secret values and credentials here. do not share this file and do not upload it to github.
    [sources.strapi]
    api_secret_key = "please set me up!" # your api_secret_key"
    domain = "please set me up!" # your strapi domain
    ```

3. When you run the Strapi project and a new tab opens in the browser, the URL in the address bar of that tab is the domain. For example, `[my-strapi.up.railway.app]` if URL in the address bar is (`http://my-strapi.up.railway.app`).
4. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Running the pipeline

1. Install the necessary dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```

2. Now you can run the pipeline using the command:
    ```bash
    python3 strapi_pipeline.py
    ```

3. To ensure that everything is loaded as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline is `strapi_pipeline`, you can use any custom name instead.



💡 To explore additional customizations for this pipeline, we recommend referring to the official dlt Strapi verified source documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the dlt Strapi documentation in [Setup Guide: Strapi.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/strapi)
