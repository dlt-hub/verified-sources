# Pipedrive

Pipedrive is a popular customer relationship management (CRM) software designed to help businesses
manage their sales processes and improve overall sales performance. Using this Pipedrive verified
source, you can load data from the Pipedrive API to your preferred destination.

The `ENTITY_MAPPINGS` table below defines mappings between different entities and their associated
fields or attributes and is taken from "[settings.py](./settings.py)" in "pipedrive" folder. Each
tuple represents a mapping for a particular entity. Here's an explanation of each tuple:

| Entity_Mappings                                | Description                                                                                                                                                                |
| ---------------------------------------------- |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ("activity", "activityFields", {"user_id": 0}) | Maps the "activity" entity to the "activityFields" representing associated fields, and the dictionary structure {"user_id": 0} sets the value of the "user_id" field to 0. |
| ("organization", "organizationFields", None)   | Maps the "organization" entity, specifying the associated fields as "organizationFields" with no additional settings or information.                                       |
| ("person", "personFields", None)               | Maps the "person" entity, specifying the associated fields as "personFields", with no additional settings or information.                                                  |
| ("product", "productFields", None)             | Maps the "product" entity, specifying the associated fields as "productFields" with no additional settings or information.                                                 |
| ("deal", "dealFields", None)                   | Maps the "deal" entity: "dealFields" represent its associated fields with no additional settings or information.                                                           |
| ("pipeline", None, None)                       | Maps the "pipeline" entity with no associated fields or additional settings.                                                                                               |
| ("stage", None, None)                          | Maps the "stage" entity with no associated fields or additional settings.                                                                                                  |
| ("user", None, None)                           | Maps the "user" entity with no associated fields or additional settings.                                                                                                   |

These entities map the fields associated with them. To get more information, please read the
[Pipedrive documentation.](https://developers.pipedrive.com/docs/api/v1)

> Note that `deals_flow` and `deals_participants` resources are built based on the `deals` resource. Therefore, loading them together in one source is a good practice. If you are using orchestrators, make sure they are requested in one task.

## Initialize the pipeline

```bash
dlt init pipedrive bigquery
```

Here, we chose BigQuery as the destination. Alternatively, you can also select Redshift, DuckDB, or
any of the [other destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations)

## Grab Pipedrive credentials

To grab the Pipedrive credentials, please refer to the following
[documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/pipedrive)

## Add credentials

1. Open `.dlt/secrets.toml`.
1. Enter the API key:
   ```toml
   # Put your secret values and credentials here
   # Note: Do not share this file or push it to GitHub!
   pipedrive_api_key = "please set me up!" # Replace this with the Pipedrive API key.
   ```
1. Enter credentials for your chosen destination [as per the docs.](https://dlthub.com/docs/dlt-ecosystem/destinations)

## Run the pipeline

1. Install requirements for the pipeline by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

1. Run the pipeline with the following command:

   ```bash
   python3 pipedrive_pipeline.py
   ```

1. To make sure that everything is loaded as expected, use the command:

   ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline is `pipedrive_pipeline`, you may also use any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official dlt
Pipedrive documentation. It provides comprehensive information and guidance on how to further
customize and tailor the pipeline to suit your specific needs. You can find the Pipedrive verified
source documentation in
[Setup Guide: Pipedrive.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/pipedrive)
