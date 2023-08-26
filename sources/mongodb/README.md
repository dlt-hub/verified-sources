# Mongo Database
Mongo is a document-oriented database that store json-like documents is one of the most used NoSQL databases. The Mongo Database `dlt` is a verified source and pipeline example that makes it easy to load data from your Mongo database to a destination of your choice. It offers flexibility in terms of loading either the entire database or specific collections to the target.

## Initialize the pipeline with Mongo Database verified source
```bash
dlt init mongodb bigquery
```
Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source

To setup the Mongo Database Verified Source read the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb)

## Add credentials
1. Open `.dlt/secrets.toml`.
2. In order to continue, you can use the a connection URL and database that looks like bellow, more details and database settings can be found in the mongodb [docs](https://www.mongodb.com/docs/manual/reference/connection-string/). If no database is provided, the default database will be used.
    ```bash
    connection_url = "mongodb://dbuser:passwd@host.or.ip:27017"
    database = "local"
    ```
    Here's what the `secrets.toml` looks like:
    ```toml
    # Put your secret values and credentials here. do not share this file and do not upload it to github.
    # We will set up creds with the following connection URL, which is a public database
    
    # The credentials are as follows
    connection_url = "mongodb://dbuser:passwd@host.or.ip:27017" # Driver name for the database
    database = "local" # Database name
    ```
3. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Running the pipeline example

1. Install the required dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```
    
2. Now you can build the verified source by using the command:
    ```bash
    python3 mongodb_pipeline.py
    ```
    
3. To ensure that everything loads as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    
    For example, the pipeline_name for the above pipeline example is `local_mongo`, you can use any custom name instead.

## How to use

You can use the mongodb verified source to load data from your Mongo database to a destination of your choice. By default all the collections in database will be loaded.

```python
from sources.mongodb import mongodb
from dlt.pipeline.pipeline import Pipeline

pipeline = dlt.pipeline(
    pipeline_name="mongo_pipeline", destination="duckdb", dataset_name="mongo_select"
)

database = mongodb()

pipeline.database(database, write_disposition="merge")
```

If you want to select the collections, you can do it by calling the `with_resources` method with the collection names as arguments.

```python
database = mongodb().with_resources("collection1", "collection2")
```

It is also possible to set the incremental parameter to load just the new records to the destination.

```python
from dlt.sources import incremental

database = mongodb(incremental=incremental("date"))
```

Or you can load a single collection using the `mongodb_collection` function.

```python
from sources.mongodb import mongodb

collection = mongodb_collection(
    collection="collection1",
    incremental=incremental(
        "lastupdated", initial_value=pendulum.DateTime(2016, 1, 1, 0, 0, 0)
    ),
)
```

💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT Mongo Database verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT Mongo Database documentation in [Setup Guide: Mongo Database.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongo_database)
