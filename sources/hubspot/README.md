# Hubspot

HubSpot is a customer relationship management (CRM) software and inbound marketing platform that helps businesses attract visitors, engage customers, and close leads.

The `dlt` HubSpot verified source allows you to automatically load data from HubSpot into a [destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) of your choice. It loads data to the following resources:

| resource                   | data                                                                     |
|----------------------------|--------------------------------------------------------------------------|
| contacts                   | visitors, potential customers, leads                                     |
| contacts_property_history  | information about historical changes in contacts properties              |
| companies                  | information about organizations                                          |
| companies_property_history | information about historical changes in companies properties             |
| deals                      | deal records, deal tracking                                              |
| deals_property_history     | information about historical changes in deals properties                 |
| products                   | goods, services                                                          |
| products_property_history  | information about historical changes in products properties              |
| tickets                    | requests for help from customers or users                                |
| tickets_property_history   | information about historical changes in tickets properties               |
| quotes                     | pricing information of a product                                         |
| quotes_property_history    | information about historical changes in quotes properties                |
| Web analytics              | events                                                                   |
| owners                     | information about account managers or users                              |
| pipelines_deals            | stages and progress tracking for deals                                   |
| stages_timing_deals            | history of entering and exiting different stages for the deals pipelines |
| pipelines_tickets          | stages and progress tracking for tickets                                 |
| stages_timing_tickets            | history of entering and exiting different stages for the tickets pipelines |
| properties                 | custom labels for properties with multiple choice                        |

## Initialize the pipeline with Hubspot verified source
```bash
dlt init hubspot duckdb
```

Here, we chose DuckDB as the destination. Alternatively, you can also choose redshift, bigquery, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Grab Hubspot credentials

To grab the Hubspot credentials, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/hubspot)

## Add credentials

1. Open `.dlt/secrets.toml`.
2. Enter the API key:

    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.hubspot]
    api_key = "api_key" # please set me up!
    ```

3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Run the pipeline

1. Install requirements for the pipeline by running the following command:

    ```bash
    pip install -r requirements.txt
    ```

2. Run the pipeline with the following command:

    ```bash
    python hubspot_pipeline.py
    ```

3. To make sure that everything is loaded as expected, use the command:

    ```bash
    dlt pipeline hubspot show
    ```


💡 To explore additional customizations for this pipeline, we recommend referring to the official `dlt` Hubspot documentation.
It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs.
You can find the `dlt` Hubspot documentation in [Setup Guide: Hubspot.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/hubspot)
