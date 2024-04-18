# Personio

> **Warning!**
>
> This source is a Community source and was tested only once. Currently, we **don't test** it on a regular basis.
> If you have any problem with this source, ask for help in our [Slack Community](https://dlthub.com/community).

[Personio](https://personio.de/) is a human resources management software that helps businesses
streamline HR processes, including recruitment, employee data management, and payroll, in one
platform.

Resources that can be loaded using this verified source are:

| Name                       | Description                                                                       | Endpoint                                          |
|----------------------------|-----------------------------------------------------------------------------------|---------------------------------------------------|
| employees                  | Retrieves company employees details                                               | /company/employees                                |
| absences                   | Retrieves absence periods for absences tracked in days                            | /company/time-offs                                |
| absences_types             | Retrieves list of various types of employee absences                              | /company/time-off-types                           |
| attendances                | Retrieves attendance records for each employee                                    | /company/attendances                              |
| projects                   | Retrieves a list of all company projects                                          | /company/attendances/projects                     |
| document_categories        | Retrieves all document categories of the company                                  | /company/document-categories                      |
| employees_absences_balance | The transformer, retrieves the absence balance for a specific employee            | /company/employees/{employee_id}/absences/balance |
| custom_reports_list        | Retrieves metadata about existing custom reports (name, report type, report date) | /company/custom-reports/reports                   |
| custom_reports             | The transformer for custom reports                                                | /company/custom-reports/reports/{report_id}       |

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

1. Next, follow the instructions in [Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) to add credentials for
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

   For example, the `pipeline_name` for the above pipeline example is `personio`, you may also use
   any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official
`dlt` [Personio](https://dlthub.com/docs/dlt-ecosystem/verified-sources/personio) documentation. It
provides comprehensive information and guidance on how to further customize and tailor the pipeline
to suit your specific needs.
