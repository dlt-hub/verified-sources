---
title: Affinity
description: dlt source for affinity.co
keywords: [Affinity API, affinity.co, CRM]
---


# Affinity

[Affinity](https://www.affinity.co/) is a CRM.


Resources that can be loaded using this verified source are:

| Name      | Description                                                            | API version | [Permissions](https://developer.affinity.co/#section/Getting-Started/Permissions) needed |
| --------- | ---------------------------------------------------------------------- | --- | --- |
| [companies](https://developer.affinity.co/#tag/companies) | The stored companies | V2 |                                                  | Requires the "Export All Organizations directory" permission. |
| [persons](https://developer.affinity.co/#tag/persons)   | The stored persons | V2 |                                                    | Requires the "Export All People directory" permission. |
| [lists](https://developer.affinity.co/#tag/lists)     | A given list and/or a saved view of a list | V2 |                            | Requires the "Export data from Lists" permission. |
| [notes](https://api-docs.affinity.co/#notes)     | Notes attached to companies, persons, opportunities | Legacy |                   | n/a |

## V1 vs V2

There are two versions of the Affinity API:
1. [Legacy](https://api-docs.affinity.co/) which is available for all plans.
2. [V2](https://developer.affinity.co/) which is only available for customers with an enterprise plan.

This verified source makes use of both API endpoints. The authentication credentials for both APIs are the same, however, they [differ in their authentication behavior](https://support.affinity.co/s/article/How-to-obtain-your-Affinity-API-key#h_01HMF147N699N2V6A9KPFMSBR6).

## Initialize the pipeline

```bash
dlt init affinity duckdb
```

Here, we chose duckdb as the destination. Alternatively, you can also choose redshift, bigquery, or
any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Add credentials

1. You'll need to [obtain your API key](https://support.affinity.co/s/article/How-to-obtain-your-Affinity-API-key) and configure the pipeline with it.

## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

2. Now the pipeline can be run by using the command:

   ```bash
   python affinity_pipeline.py
   ```

3. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline affinity_pipeline show
   ```

💡 To explore additional customizations for this pipeline, we recommend referring to the official
`dlt` Affinity documentation. It provides comprehensive information and guidance on how to further
customize and tailor the pipeline to suit your specific needs. You can find the `dlt` Affinity
documentation in
[Setup Guide: Affinity](https://dlthub.com/docs/dlt-ecosystem/verified-sources/affinity).
