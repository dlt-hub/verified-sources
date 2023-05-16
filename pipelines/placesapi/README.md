---
title: Places API
description: dlt pipeline for Places API
keywords: [places api, places pipeline]
---

# Places API

The pipleline uses [Google Places API](https://developers.google.com/maps/documentation/places/web-service/overview) to search information about different locations, and loads data into [Google BigQuery](https://dlthub.com/docs/destinations/bigquery).


## Initialize the pipeline

Initialize the pipeline with the following command:

```
dlt init placesapi bigquery
```

Running this command will create a directory with the following structure:

```bash
├── .dlt
│   ├── .pipelines
│   ├── config.toml
│   └── secrets.toml
├── placesapi
│   └── __init__.py
├── .gitignore
├── placesapi_pipeline.py
└── requirements.txt
```

## Add credentials

Before running the pipeline you may need to add credentials in the `.dlt/secrets.toml` file for Google BigQuery. For instructions on how to do this, follow the steps detailed under the [destination:BigQuery](https://dlthub.com/docs/destinations/bigquery) page.

## Run the pipeline

1. Install the necessary dependencies by running the following command:
```
pip install -r requirements.txt
```
2. Now the pipeline can be run by using the command:
```
python3 placesapi_pipeline.py
```
3. To make sure that everything is loaded as expected, use the command:
```
dlt pipeline placesapi_pipeline show
```