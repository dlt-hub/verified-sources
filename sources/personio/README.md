# Personio

[Personio](https://personio.de/) is a human resources management software that helps businesses 
streamline HRprocesses, including recruitment, employee data management, and payroll, in one 
platform.

Resources that can be loaded using this verified source are:

| Name        | Description                                          |
|-------------|------------------------------------------------------|
| employees   | Retrieves company employees details                  |
| absences    | Retrieves list of various types of employee absences |
| attendances | Retrieves attendance records for each employee       |
## Initialize the pipeline

```bash
dlt init personio duckdb
```

Here, we chose `duckdb` as the destination. Alternatively, you can also choose `redshift`,
`bigquery`, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Setup verified source

To grab Personio credentials and configure the verified source, please refer to the
[full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/personio#grab-credentials)

## Add credential

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe. Here's what the file looks like:

   ```toml
   # Put your secret values and credentials here
   # Note: Do not share this file and do not push it to GitHub!
   [sources.personio]
   client_id = "papi-*****" # please set me up!
   client_secret = "papi-*****" # please set me up!
   ```

1. Replace the value of `client_id` and `client_secret`. This will ensure that you can access
   Personio API securely.

1. Next, follow the instructions in [Destinations](../destinations/duckdb) to add credentials for
   your chosen destination. This will ensure that your data is properly routed to its final
   destination.

## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

1. Now the pipeline can be run by using the command:

   ```bash
   python personio_pipeline.py
   ```

1. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `personio`, you may also use
   any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official
`dlt` [Personio](https://dlthub.com/docs/dlt-ecosystem/verified-sources/personio) documentation. It
provides comprehensive information and guidance on how to further customize and tailor the pipeline
to suit your specific needs.
