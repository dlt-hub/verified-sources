---
title: Airtable
description: dlt source for Airtable.com API
keywords: [airtable api, airtable source, airtable, base]
---


# Airtable

[Airtable](https://airtable.com/) is a spreadsheet-database hybrid, with the features of a database but applied to a spreadsheet. It is also marketed as a low‒code platform to build next‒gen apps. Spreadsheets are called *airtables* and are grouped into *bases*. 
Records can link to each other.

This [dlt source](https://dlthub.com/docs/general-usage/source) creates a [dlt resource](https://dlthub.com/docs/general-usage/resource) for every airtable and loads it into the destination. 


## Supported methods to load airtables

1. identify the *base* you want to load from. The ID starts with "app". See [how to find airtable IDs](https://support.airtable.com/docs/finding-airtable-ids)
2. identify the *airtables* you want to load. You can identify in three ways:

   1. retrieve *airtables* from a given *base* for a list of user-defined mutable names of tables
   2. retrieve *airtables* from a given *base* for a list of immutable IDs defined by airtable.com at the creation time of the *airtable*. IDs start with "tbl". See [how to find airtable IDs](https://support.airtable.com/docs/finding-airtable-ids)
   3. empty filter: retrieve all *airtables* from a given *base*


## Supported write dispositions
This connector supports the write disposition `replace`, i.e. it does a [full load](https://dlthub.com/docs/general-usage/full-loading) on every invocation.

To use support `append`, i.e. [incremental loading](https://dlthub.com/docs/general-usage/incremental-loading) there are two possibilities:

### Parametrize the `pipeline.run` method 

```python
 event_base = airtable_source(
     base_id="app7RlqvdoOmJm9XR",
     table_names=["💰 Budget"],
 )
 load_info = pipeline.run(event_base, write_disposition="replace")
```

## Customize the resource using the `apply_hints` method

This approach further allows to [adjust the schema](https://dlthub.com/docs/general-usage/resource#adjust-schema)
```python
 event_base = airtable_source(
     base_id="app7RlqvdoOmJm9XR",
     table_names=["💰 Budget"],
 )
 event_base.resources["💰 Budget"].apply_hints(
      write_disposition="merge",
      columns={"Item": {"name": "Item", "data_type": "text"}},
 )
 load_info = pipeline.run(event_base)
```


## Initialize the pipeline

```bash
dlt init airtable duckdb
```

Here, we chose duckdb as the destination. Alternatively, you can also choose redshift, bigquery, or any of the other 
[destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).


## Add credentials

1. [Obtain a Personal Access Token](https://support.airtable.com/docs/creating-and-using-api-keys-and-access-tokens).
  If you're on an enterprise plan you can create an [Airtable Service Account](https://dlthub.com/docs/dlt-ecosystem/verified-sources/chess).
  Place the key into your `secrets.toml` like this:
```
[sources.airtable]
access_token = "pat***"
```
2. Follow the instructions in the [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) 
  document to add credentials for your chosen destination.


## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

2. Now the pipeline can be run by using the command:

   ```bash
   python airtable_pipeline.py
   ```

3. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline airtable_pipeline show
   ```

💡 To explore additional customizations for this pipeline, we recommend referring to the example pipelines in `airtable_pipeline.py`.