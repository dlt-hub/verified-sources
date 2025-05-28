# Quickbooks

QuickBooks is a cloud-based accounting software designed for small to medium-sized businesses. This QuickBooks `dlt` verified source and pipeline example offers the capability to load QuickBooks endpoints such as "Customer" to a destination of your choosing. It enables you to conveniently load the following endpoint as a start:

### Single loading endpoints (replace mode)

| Endpoint | Mode | Description |
| --- | --- | --- |
| Customer | replace | A customer is a consumer of the service or product that your business offers. An individual customer can have an underlying nested structure, with a parent customer (the top-level object) having zero or more sub-customers and jobs associated with it. |


## Initialize the pipeline with Quickbooks verified source
```bash
dlt init quickbooks_online duckdb
```

Here, we chose DuckDB as the destination. Alternatively, you can also choose redshift, snowflake, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source and pipeline example

To grab credentials and initialize the verified source, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/salesforce)

## Add credentials

1. Open `.dlt/secrets.toml`.
2. Put these credentials in, these can be sourced from quickbooks developer portal and quickbooks oauth playground:
    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.quickbooks_online]
    company_id=""
    client_id=""
    client_secret=""
    refresh_token=""
    redirect_url=""
    ```

3. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```

2. Now the pipeline can be run by using the command:
    ```bash
    python3 quickbooks_online_pipeline.py
    ```

3. To make sure that everything is loaded as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline is `quickbooks_online`, you may also use any custom name instead.

