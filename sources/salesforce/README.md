# Salesforce

Loads Salesforce sObjects with the Bulk API 2.0, and with the REST API for sObjects the Bulk API does not support.

- `salesforce_source` loads User, UserRole, Opportunity, OpportunityLineItem, OpportunityContactRole, Account, Contact, Lead, Campaign, CampaignMember, Product2, Pricebook2, PricebookEntry, Task and Event.
- `salesforce_table` loads any sObject, including custom objects.

By default records changed since the last run are merged on `Id` and records deleted in Salesforce are deleted from the destination. `start_date` sets where the first run starts. `write_disposition="replace"` reloads all records, or the records changed since `start_date`. Field types come from the sObject description. `pyarrow` is optional: it is used when installed and is much faster on large sObjects.

Creating the source or a resource logs in and describes the sObjects. On Airflow, where DAG files are parsed often, pass `defer_table_reflect=True` so this happens only when the data is extracted.

> Upgrading from an earlier version of this source changes column types, primary keys and write dispositions. Reload the data with `pipeline.run(salesforce_source(), refresh="drop_sources")`.

Incremental loading has limits that come from Salesforce:
- Formula fields that read related records or use `TODAY()` change without changing `SystemModstamp`, so loaded rows keep old values. Reload the sObject to refresh them.
- Account and opportunity team members, and sharing changes in general, do not change `SystemModstamp`.
- Deleted records stay in the recycle bin for 15 days. Records deleted longer than that before a run stay in the destination.

## Changelog

### 1.0.0
- **breaking** `salesforce_source`: the `incremental` argument is removed. Pass `write_disposition="replace"` to reload all records
- **breaking** `salesforce_table`: `incremental=None` uses the default incremental instead of reloading all records. Pass `write_disposition="replace"` to reload all records
- new `start_date` argument of `salesforce_source` and `salesforce_table`: the initial value of the default incremental
- fixed resources resolve config in the `salesforce` section instead of `table`
- fixed incremental queries on a timestamp read records changed until the query starts. Records changed while a long query job runs are loaded by the next run instead of being skipped
- fixed fields longer than 131,072 characters, like Apex code, with the `python` backend and records over 2 MiB with the `pyarrow` backend
- requests refused because the org runs too many long requests at once are retried
- a query job that does not complete in 24 hours is aborted and raises
- a result page with fewer records than Salesforce counted raises
- `api="auto"` also reads sObjects with compound fields the Bulk API cannot select with the REST API

## Initialize the pipeline with Salesforce verified source
```bash
dlt init salesforce bigquery
```

Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source and pipeline example

To initialize the verified source, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/salesforce)

## Add credentials and configs

1. Open `.dlt/secrets.toml`.
2. Enter the username, password and security token:
    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.salesforce.credentials]
    user_name = "please set me up!" # Salesforce user name
    password = "please set me up!" # Salesforce password
    security_token = "please set me up!" # Salesforce security token generated
    ```

   Alternatively, you may authenticate using any method supported by the underlying `simple_salesforce` library. For instructions on obtaining these credentials, please consult Salesforce’s documentation.

    - IP-whitelist + organization ID:
        ```toml
        [sources.salesforce.credentials]
        user_name = "please set me up!" 
        password = "please set me up!" 
        organization_id = "please set me up!"
        ```    

    - Session ID + instance:
        ```toml
        [sources.salesforce.credentials]
        session_id = "please set me up!" 
        instance = "please set me up!" # OR
        instance_url = "please set me up!"
        ```    

    - JWT method:
        ```toml
        [sources.salesforce.credentials]
        user_name = "please set me up!" 
        consumer_key = "please set me up!" 
        privatekey_file = "please set me up!" # OR
        privatekey = "please set me up!"
        instance_url = "please set me up!" # Optional
        ```    

    - Connected app method:
        ```toml
        [sources.salesforce.credentials]
        user_name = "please set me up!" 
        password = "please set me up!" 
        consumer_key = "please set me up!"
        consumer_secret = "please set me up!"
        ```    
    
    - Client credentials method:
        ```toml
        [sources.salesforce.credentials]
        consumer_key = "please set me up!"
        consumer_secret = "please set me up!"
        domain = "please set me up!"
        ```    

3. You can set up the following optional configs in your `.dlt/config.toml` file that will be used to connect to Salesforce:

    ```toml
    [sources.salesforce]
    domain = "please set me up!" # Set to "test" if sandbox
    version = "please set me up!" # Version of the Salesforce API to use, defaults to 59.0 
    proxies = "please set me up!" # Mapping of protocols to proxy servers
    client_id = "please set me up!" # App identifier for Salesforce API usage tracking
    ```

4. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```
    
2. Now the pipeline can be run by using the command:
    ```bash
    python3 salesforce_pipeline.py
    ```
    
3. To make sure that everything is loaded as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    
    For example, the pipeline_name for the above pipeline is `salesforce`, you may also use any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official dlt Salesforce verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the dlt Salesforce documentation in [Setup Guide: Salesforce.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/salesforce)
    
