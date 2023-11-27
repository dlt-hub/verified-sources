---
title: Personio
description: dlt source for Personio API
keywords: [personio api, personio source, personio]
---


# Personio

[Personio](https://personio.de/) is a holistic HR software for companies from 10 - 2000 employees.
It is one of the relevant data sources for our organisation.

Resources that can be loaded using this verified source are:

| Name        | Description                                                                                                                  |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------- |
| employees   | List Company Employees using the [employees](https://api.personio.de/v1/company/employees) endpoint.                         |
| absences    | Provides a list of absence types using the [time-off-types](https://api.personio.de/v1/company/time-off-types) endpoint.     |
| attendances | Fetch attendance data for the company employees using [attendances](https://api.personio.de/v1/company/attendances) endpoint.|

## Initialize the pipeline

```bash
dlt init personio duckdb
```

Here, we chose duckdb as the destination. Alternatively, you can also choose redshift, bigquery, or
any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Add credentials

1. Get your Personio `client_id` and `client_secret`. The process is detailed in the
   [docs](https://developer.personio.de/docs#21-employee-attendance-and-absence-endpoints).
2. Open `.dlt/secrets.toml`.
3. Enter the API `client_id` and `client_secret`:

   ```toml
   [sources.personio]
   client_id = 'papi-********-****-****-****-************'
   client_secret = 'papi-************************************************'
    ```
4. Follow the instructions in the
   [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) document to add credentials
   for your chosen destination.


## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

2. Now the pipeline can be run by using the command:

   ```bash
   python3 personio_pipeline.py
   ```

3. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline personio_pipeline show
   ```

💡 To explore additional customizations for this pipeline, we recommend referring to the official
`dlt` Chess documentation. It provides comprehensive information and guidance on how to further
customize and tailor the pipeline to suit your specific needs. You can find the `dlt` Chess
documentation in
[Setup Guide: Personio.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/personio)
