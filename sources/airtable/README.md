---
title: Airtable
description: dlt source for Airtable.com API
keywords: [airtable api, airtable source, airtable, base]
---

# Airtable

[Airtable](https://airtable.com/) is a cloud-based platform that merges spreadsheet and database
functionalities for easy data management and collaboration.

Sources and resources that can be loaded using this verified source are:

| Name              | Description                                |
| ----------------- | ------------------------------------------ |
| airtable_source   | Retrieves tables from an airtable base     |
| airtable_resource | Retrives data from a single airtable table |

## Initialize the pipeline

```bash
dlt init airtable duckdb
```

Here, we chose duckdb as the destination. Alternatively, you can also choose redshift, bigquery, or
any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Grab Airtable credentials

To learn about grabbing the Airtable credentials and configuring the verified source, please refer
to the
[full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/airtable#grab-airtable-ids)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   [sources.airtable]
   access_token = "Please set me up!" # please set me up!
   ```

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

1. Next you need to configure ".dlt/config.toml", which looks like:

   ```toml
   [sources.airtable]
   base_id = "Please set me up!"       # The id of the base.
   table_names = ["Table1","Table2"]   # A list of table IDs or table names to load.
   ```

   > Optionally, you can also input "base_id" and "table_names" in the script, as in the pipeline
   > example.

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```bash
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```bash
   python airtable_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `airtable`, you may also use
   any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT
Airtable documentation. It provides comprehensive information and guidance on how to further
customize and tailor the pipeline to suit your specific needs. You can find the DLT Airtable
documentation in the
[Setup Guide: Airtable.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/airtable)
