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
    connection_url = "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    ```
    Here's what the `secrets.toml` looks like:
    ```toml
    # Put your secret values and credentials here. do not share this file and do not upload it to github.
    # We will set up creds with the following connection URL, which is a public database

    # The credentials are as follows
    drivername = "mysql+pymysql" # Driver name for the database
    database = "Rfam # Database name
    username = "rfamro" # username associated with the database
    host = "mysql-rfam-public.ebi.ac.uk" # host address
    port = "4497 # port required for connection
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

    For example, the pipeline_name for the above pipeline example is `rfam`, you can use any custom name instead.


## Pick right table backend for your data
You can

**sqlalchemy** (the default) yields table data as list of Python dictionaries. This data goes through regular extract
and normalize steps and does not require additional dependencies to be installed. It is the most robust (works with any destination, correctly represents data types) but also the slowest.
:::tip
Use it during development or during the first time setup and then switch to **pyarrow**.
:::

**pyarrow** yields data as Arrow tables. Uses **SqlAlchemy** to read rows in chunks


uses SqlAlchemy to read rows in chunks but then converts rows into numpy ndarray (using Pandas if available) and from there into Arrow table.
`dlt` will create arrow schema from reflected table metadata and will preserve data types ie. **decimal** / **numeric** will be extracted without loss of precision.
If the destination loads parquet files, this backend will skip `dlt` normalizer and you can gain two orders of magnitude (20x - 30x) speed increase.

```py

```



💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT SQL Database verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT SQL Database documentation in [Setup Guide: SQL Database.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database)

