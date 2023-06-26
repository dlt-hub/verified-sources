# Shopify

Shopify is an easy-to-use e-commerce solution that allows anyone to create and manage their own online store. This `dlt` verified source is designed to efficiently load data from multiple endpoints that are:

| Endpoint | Description |
| --- | --- |
| customers | individuals or entities who have created accounts on a Shopify-powered online store |
| orders  | transactions made by customers on an online store |
| products | the individual items or goods that are available for sale |

## Initialize the pipeline

```bash
dlt init shopify_dlt bigquery
```

Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Grab credentials

To grab your Shopify credentials and initialise your verified source, see the [full documentation here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/shopify).

## Add credentials

1. Open `.dlt/secrets.toml`.
2. Enter the API access token:

    ```toml
    [sources.shopify]
    private_app_password=" Please set me up !" #Admin API access token
    ```

3. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)
4. Inside the `.dlt` folder, you'll find another file called `config.toml`, where you can store your Shopify URL. The `config.toml` looks like this:
    ```toml
    [sources.shopify_dlt]
    shop_url = "Please set me up!
    ```

5. Replace `shop_url` with the URL of your Shopify store. For example "https://shop-123.myshopify.com/".

## Running the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:
    ```bash
    pip install -r requirements.txt
    ```

2. Now the pipeline can be run by using the command:
    ```bash
    python3 shopify_dlt_pipeline.py
    ```
3. To make sure that everything is loaded as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline is `shopify`, you may also use any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT Shopify verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT Shopify documentation in [Setup Guide: Shopify](https://dlthub.com/docs/dlt-ecosystem/verified-sources/shopify).

