# SQL Database
SQL database, or Structured Query Language database, are a type of database management system (DBMS) that stores and manages data in a structured format.  The SQL Database `dlt` is a verified source and pipeline example that makes it easy to load data from your SQL database to a destination of your choice. It offers flexibility in terms of loading either the entire database or specific tables to the target.

## Initialize the pipeline with SQL Database verified source
```bash
dlt init sql_database bigquery
```
Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source

To setup the SQL Database Verified Source read the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database)

## Add credentials
1. Open `.dlt/secrets.toml`.
2. In order to continue, we will use the supplied connection URL to establish credentials. The connection URL is associated with a public database and looks like this:
    ```bash
    connection_url = "mysql+pymysql://anonymous@ensembldb.ensembl.org:3306/acanthochromis_polyacanthus_core_100_1"
    ```
    Here's what the `secrets.toml` looks like:
    ```toml
    # Put your secret values and credentials here. do not share this file and do not upload it to github.
    # We will set up creds with the following connection URL, which is a public database
    
    # The credentials are as follows
    drivername = "mysql+pymysql" # Driver name for the database
    database = "acanthochromis_polyacanthus_core_100_1" # Database name
    username = "anonymous" # username associated with the database
    host = "anonymous@ensembldb.ensembl.org" # host address
    port = "3306" # port required for connection
    ```
3. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Running the pipeline example

1. Install the required dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```
    
2. Now you can build the verified source by using the command:
    ```bash
    python3 sql_database_pipeline.py
    ```
    
3. To ensure that everything loads as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    
    For example, the pipeline_name for the above pipeline example is `ensembl`, you can use any custom name instead.
    


💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT SQL Database verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT SQL Database documentation in [Setup Guide: SQL Database.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database)

