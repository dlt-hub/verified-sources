# Jira

[Jira](https://www.atlassian.com/software/jira) by Atlassian helps teams manage projects and tasks
efficiently, prioritize work, and collaborate.

Resources that can be loaded using this verified source are:

| Name      | Description                                                                              |
| --------- | ---------------------------------------------------------------------------------------- |
| issues    | individual pieces of work to be completed                                                |
| users     | administrator of a given project                                                         |
| workflows | the key aspect of managing and tracking the progress of issues or tasks within a project |
| projects  | a collection of tasks that need to be completed to achieve a certain outcome             |

## Initialize the pipeline

```bash
dlt init jira duckdb
```

Here, we chose `duckdb` as the destination. Alternatively, you can also choose `redshift`,
`bigquery`, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Setup verified source

To grab Jira credentials and configure the verified source, please refer to the
[full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira#setup-guide)

## Add credential

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe. Here's what the file looks like:

   ```toml
   # put your secret values and credentials here. Please do not share this file, and do not push it to GitHub
   [sources.jira]
   subdomain = "set me up!" # please set me up!
   email = "set me up!" # please set me up!
   api_token = "set me up!" # please set me up!
   ```

1. A subdomain in a URL identifies your Jira account. For example, in
   "https://example.atlassian.net", "example" is the subdomain.

1. Use the email address associated with your Jira account.

1. Set up the "access_token" to ensure secure access to your Jira account.

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
   python jira_pipeline.py
   ```

1. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `jira_pipeline`, you may also
   use any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official
`dlt` [Jira](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira) documentation. It provides
comprehensive information and guidance on how to further customize and tailor the pipeline to suit
your specific needs.
